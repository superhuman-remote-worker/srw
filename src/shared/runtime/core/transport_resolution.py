"""Transport-resolvability policy (shared, dependency-free).

Mirrors the raise conditions in ``src/shared/runtime/core/loader.py``'s ``create_llm``
factories so the orchestrator dispatch pre-flight and the agent loader agree on
what counts as a *usable* transport — without the orchestrator importing the
full loader (which pulls ``aiosqlite``, absent in the orchestrator image). Pure
stdlib only.

The point: a session/role that can never start (a chat model whose provider
factory will raise for a missing key, or an embedding endpoint the memory
reranker rides but can't reach) should be rejected BEFORE a pod is spawned,
with an actionable reason — not crash the agent at startup and hang the UI.
See knowledge-base/knowledge/issues/openrouter_auxiliary_crashes_session_via_memory_reranker.md.

Per-provider base_url / api_key behaviour, from the loader factories:

- ``openrouter`` / ``mistral`` / ``anthropic`` / ``google`` / ``groq``: supply a
  built-in default base_url, so the endpoint is never the problem — but they
  RAISE when no api_key resolves (``config.api_key or os.getenv(<PROVIDER>_API_KEY)``).
- ``openai`` (and the unset default): ``base_url = config.base_url`` (None →
  api.openai.com); the key falls back to a ``"not-needed"`` sentinel and never
  raises. So a keyless/self-hosted openai-shaped endpoint is valid (no key check)
  and only a *missing* base_url on an endpoint-backed model is wrong — but that
  isn't distinguishable from a genuine OpenAI model at this layer, so it is not
  flagged here (it 401s at runtime, it does not crash startup).
- ``codex``: default base_url (CODEX proxy), keyless. Never flagged.
"""

from dataclasses import dataclass
from typing import Mapping, Optional
from urllib.parse import urlsplit

# Providers whose create_llm factory RAISES when no api_key resolves.
KEY_REQUIRED_PROVIDERS = frozenset(
    {"openrouter", "mistral", "anthropic", "google", "groq"}
)

# The env var each provider's factory falls back to (config.api_key or getenv).
PROVIDER_ENV_KEY = {
    "openrouter": "OPENROUTER_API_KEY",
    "mistral": "MISTRAL_API_KEY",
    "anthropic": "ANTHROPIC_API_KEY",
    "google": "GOOGLE_API_KEY",
    "groq": "GROQ_API_KEY",
    "openai": "OPENAI_API_KEY",
}


def llm_role_violation(
    role: str, section: Mapping, *, env: Optional[Mapping] = None
) -> Optional[str]:
    """Reason string if an LLM role's resolved transport is unusable (would raise
    in ``create_llm``), else ``None``.

    ``section`` is the credential-injected config-override dict for the role
    (e.g. ``blob['agent']['llm']`` / ``blob['agent']['auxiliary']``). ``env``
    should combine the delivery blob's ``env_keys`` with the process
    environment, since the orchestrator injects some provider keys into
    ``env_keys`` (e.g. ``OPENROUTER_API_KEY``) rather than onto the section —
    which is exactly why an OpenRouter chat model builds while only the reranker
    crashed in the original incident.
    """
    if env is None:
        import os

        env = os.environ
    if not isinstance(section, Mapping):
        return None
    provider = (section.get("provider") or "openai").lower()
    model = section.get("model") or "?"
    api_key = section.get("api_key")
    if provider in KEY_REQUIRED_PROVIDERS:
        env_key = PROVIDER_ENV_KEY.get(provider, "")
        if not api_key and not (env_key and env.get(env_key)):
            return (
                f"{role} model '{model}' ({provider}) has no usable api_key — "
                f"none injected and {env_key} is unset; the agent's create_llm "
                f"will raise at startup"
            )
    return None


def embedding_role_violation(
    env_keys: Optional[Mapping],
) -> Optional[str]:
    """Reason string if the *configured* embedding transport is unusable, else
    ``None``.

    The memory reranker rides the embedding endpoint (``EMBEDDING_BASE_URL``), so
    an unresolvable embedding transport now means the reranker can't build either
    — the exact class of the original crash. Only ``env_keys`` is consulted (not
    the process env): a session that carries no ``EMBEDDING_MODEL`` override
    relies on the agent pod's cluster-default embedding env, which the
    orchestrator can't see and must not second-guess (no false rejects). We flag
    only the unambiguous case: the delivery blob resolved an embedding *model*
    but no *endpoint* for it (a decrypt-miss or a missing endpoint base_url).
    """
    if not env_keys:
        return None
    model = env_keys.get("EMBEDDING_MODEL")
    if not model:
        return None  # not overridden → cluster default, don't second-guess
    provider = (env_keys.get("EMBEDDING_PROVIDER") or "local").lower()
    base_url = env_keys.get("EMBEDDING_BASE_URL")
    if provider in ("local", "openai") and not base_url:
        return (
            f"embedding model '{model}' ({provider}) resolved but no "
            f"EMBEDDING_BASE_URL — memory, KB, and the reranker cannot reach the "
            f"embedding endpoint"
        )
    return None


def rerank_role_violation(
    env_keys: Optional[Mapping],
) -> Optional[str]:
    """Reason string if the *configured* rerank transport is unusable, else
    ``None``.

    Mirrors :func:`embedding_role_violation` for the ``rerank`` catalog slot.
    The memory reranker binds ``RERANK_BASE_URL`` when dispatch resolved one
    and otherwise rides the embedding endpoint (``EMBEDDING_BASE_URL``), so
    the unambiguous failure is: a rerank *model* was delivered but neither
    endpoint was — the scorer's ``ValueError`` at bind, one pod-spawn later.
    A delivery with no ``RERANK_MODEL`` is the cluster default and is not
    second-guessed here.
    """
    if not env_keys:
        return None
    model = env_keys.get("RERANK_MODEL")
    if not model:
        return None
    if env_keys.get("RERANK_BASE_URL") or env_keys.get("EMBEDDING_BASE_URL"):
        return None
    return (
        f"rerank model '{model}' resolved but no RERANK_BASE_URL (and no "
        f"EMBEDDING_BASE_URL to fall back to) — the memory reranker cannot "
        f"reach a /rerank endpoint"
    )


# ---------------------------------------------------------------------------
# Env-key endpoint names
# ---------------------------------------------------------------------------
#
# Every env name a reader consults for the endpoint of a ``{PREFIX}_*`` flat
# capability, in reader precedence order. The dispatch injector derives its
# "a key follows its endpoint" guard from this same table, so a reader that
# grows a new alias cannot drift away from the guard.
ENV_ENDPOINT_ALIASES: dict[str, tuple[str, ...]] = {
    "CITATION_LLM": ("CITATION_LLM_BASE_URL", "CITATION_LLM_URL"),
}


_DEFAULT_PORTS = {"http": 80, "https": 443}


def endpoint_origin(url: Optional[str]) -> Optional[tuple[str, str, int]]:
    """``(scheme, host, port)`` of ``url``, lower-cased with default ports
    filled in; ``None`` for an empty or unparseable URL. Path, query and case
    are ignored, so ``https://OpenRouter.ai/api`` and
    ``https://openrouter.ai/api/v1/`` share one origin."""
    if not url or not str(url).strip():
        return None
    raw = str(url).strip()
    if "://" not in raw:
        raw = "https://" + raw
    try:
        parts = urlsplit(raw)
        port = parts.port
    except ValueError:
        return None
    scheme = (parts.scheme or "").lower()
    host = (parts.hostname or "").lower()
    if not host:
        return None
    return scheme, host, port if port is not None else _DEFAULT_PORTS.get(scheme, 0)


def same_endpoint(a: Optional[str], b: Optional[str]) -> bool:
    """True when two URLs name the same origin (scheme + host + port).

    The canonical-host check behind "a key follows its endpoint": the chat
    factories' env fallback compares a configured URL with a provider's own
    origin, where any path on that origin reaches the provider. Two-URL
    comparisons on possibly shared hosts use :func:`endpoint_within`, which
    also compares paths. Two unset URLs are the same (both the provider
    default).
    """
    origin_a, origin_b = endpoint_origin(a), endpoint_origin(b)
    if origin_a is None or origin_b is None:
        return origin_a is None and origin_b is None
    return origin_a == origin_b


def _normalised_path(url: str) -> tuple[str, ...]:
    raw = str(url).strip()
    if "://" not in raw:
        raw = "https://" + raw
    try:
        path = urlsplit(raw).path
    except ValueError:
        path = ""
    segments = tuple(seg for seg in path.lower().split("/") if seg)
    # A leading or trailing ``/v1`` is the API-version marker, optional by
    # convention; a ``v1`` anywhere else is part of the tenant path.
    if segments and segments[0] == "v1":
        segments = segments[1:]
    if segments and segments[-1] == "v1":
        segments = segments[:-1]
    return segments


def endpoint_within(candidate: Optional[str], parent: Optional[str]) -> bool:
    """True when ``candidate`` is the ``parent`` endpoint or below it.

    For the two-URL comparisons — an overlay/child/reranker URL against the
    endpoint whose key it would reuse. Shared hosts tenant by path (e.g. an
    AI gateway's ``/v1/<account>/<gateway>/``), so the same origin is not
    enough: the candidate's normalised path (lower-case, trailing slash and a
    trailing ``/v1`` stripped) must equal or extend the parent's. A sibling
    path never matches. Two unset URLs match (both the provider default).
    """
    if not same_endpoint(candidate, parent):
        return False
    if candidate is None or parent is None or not str(candidate).strip():
        return True
    cand, base = _normalised_path(candidate), _normalised_path(parent)
    return cand[: len(base)] == base


def env_endpoint_names(prefix: str) -> tuple[str, ...]:
    """Env names that carry the endpoint for ``prefix`` (reader order)."""
    return ENV_ENDPOINT_ALIASES.get(prefix, (f"{prefix}_BASE_URL",))


# The flat capabilities the orchestrator's dispatch injectors write into a
# delivered ``env_keys`` block — the only names an agent may export from it
# into its process environment. Anything else (an arbitrary SDK variable) is
# dropped at the export seam.
_DISPATCH_ENV_PREFIXES = (
    "EMBEDDING",
    "KB_EMBEDDING",
    "RERANK",
    "VISION",
    "WHISPER",
    "TTS",
    "CITATION_LLM",
)
_DISPATCH_ENV_SUFFIXES = (
    "MODEL",
    "BASE_URL",
    "API_KEY",
    "PROVIDER",
    "PROFILE_ID",
    "DIMENSIONS",
)
# Non-secret tuning an owner may set through a project override's env_keys.
# Each carries neither a key nor a URL, so it cannot move a credential.
_TUNING_ENV_KEY_NAMES = frozenset(
    {
        "WHISPER_LANGUAGE",
        "WHISPER_TIMEOUT",
        "VISION_TIMEOUT",
        "EMBEDDING_MAX_BATCH",
        "EMBEDDING_MAX_BATCH_SIZE",
        "CITATION_REASONING_REQUIRED",
        "OPENROUTER_REFERER",
        "OPENROUTER_TITLE",
    }
)
DISPATCH_ENV_KEY_NAMES = frozenset(
    {f"{p}_{s}" for p in _DISPATCH_ENV_PREFIXES for s in _DISPATCH_ENV_SUFFIXES}
    | {name for names in ENV_ENDPOINT_ALIASES.values() for name in names}
    | {"OPENROUTER_API_KEY"}
    | _TUNING_ENV_KEY_NAMES
)


def exportable_env_keys(
    env_keys: Optional[Mapping],
) -> tuple[dict[str, str], list[str]]:
    """Split a delivered ``env_keys`` block into (exportable, dropped names).

    Only :data:`DISPATCH_ENV_KEY_NAMES` with a string value are exportable.
    The dropped list carries names only, for a log line.
    """
    kept: dict[str, str] = {}
    dropped: list[str] = []
    for name, value in (env_keys or {}).items():
        if name in DISPATCH_ENV_KEY_NAMES and isinstance(value, str):
            kept[name] = value
        else:
            dropped.append(str(name))
    return kept, sorted(dropped)


# ---------------------------------------------------------------------------
# Citation LLM credential isolation
# ---------------------------------------------------------------------------
#
# The citation verifier is a second, independently-endpointed OpenAI-shaped
# client (``CITATION_LLM_MODEL`` / ``CITATION_LLM_{BASE_}URL`` /
# ``CITATION_LLM_API_KEY``, dispatched by the orchestrator or set in ``.env``).
# ``OPENAI_API_KEY`` belongs to the *chat* model and may only ever be sent to
# api.openai.com itself — never to whatever host the citation URL names. See
# knowledge-base/knowledge/issues/citation_llm_api_key_isolation.md.

# Hosts that ARE OpenAI — the only endpoints ``OPENAI_API_KEY`` is sent to
# implicitly. Anything else is third-party / self-hosted and must carry its own
# ``CITATION_LLM_API_KEY``.
OPENAI_DEFAULT_HOSTS = frozenset({"api.openai.com"})


class CitationTransportError(ValueError):
    """The citation LLM endpoint is set but carries no credential of its own.

    Raised by :func:`resolve_citation_transport` instead of falling back to
    ``OPENAI_API_KEY`` for a non-OpenAI endpoint. The agent catches it, logs the
    misconfiguration, and degrades citation verification to the auxiliary model
    (the same path any other dedicated-client build failure takes).
    """


@dataclass(frozen=True)
class CitationTransport:
    """Resolved citation-client endpoint + credential.

    ``key_source`` names where ``api_key`` came from (``"CITATION_LLM_API_KEY"``,
    ``"OPENAI_API_KEY"`` or ``"none"``) so the agent can log *which* key it is
    using without logging the key.
    """

    base_url: Optional[str]
    api_key: Optional[str]
    key_source: str


def is_openai_default_endpoint(base_url: Optional[str]) -> bool:
    """True when ``base_url`` is unset or names api.openai.com (the SDK default).

    Only these endpoints may receive ``OPENAI_API_KEY`` implicitly. A URL that
    cannot be parsed to a hostname is treated as custom (fail closed).
    """
    if not base_url or not base_url.strip():
        return True
    raw = base_url.strip()
    if "://" not in raw:
        raw = "//" + raw
    try:
        host = urlsplit(raw).hostname
    except ValueError:
        return False
    return (host or "").lower() in OPENAI_DEFAULT_HOSTS


def resolve_citation_transport(env: Optional[Mapping] = None) -> CitationTransport:
    """Resolve the citation client's endpoint and credential from ``env``.

    Precedence:

    1. ``CITATION_LLM_API_KEY`` set (non-empty) → it is the key, whatever the
       endpoint. ``CITATION_LLM_BASE_URL`` (orchestrator name) wins over
       ``CITATION_LLM_URL`` (``.env`` name) for the endpoint.
    2. No dedicated key and the endpoint is api.openai.com / unset → today's
       behaviour: ``OPENAI_API_KEY`` (or ``None``, which the openai factory
       turns into its ``"not-needed"`` sentinel).
    3. No dedicated key and a custom endpoint → :class:`CitationTransportError`.
       ``OPENAI_API_KEY`` is never sent to a host the operator did not name it
       for; a keyless self-hosted server wants ``CITATION_LLM_API_KEY=not-needed``.

    Empty-string values count as unset (the dispatcher never injects a
    half-credential, but a ``.env`` line like ``CITATION_LLM_API_KEY=`` does).
    """
    if env is None:
        import os

        env = os.environ
    names = env_endpoint_names("CITATION_LLM")
    url_var = next((name for name in names if env.get(name)), names[-1])
    base_url = env.get(url_var) or None
    dedicated = env.get("CITATION_LLM_API_KEY") or None
    if dedicated:
        return CitationTransport(
            base_url=base_url, api_key=dedicated, key_source="CITATION_LLM_API_KEY"
        )
    if not is_openai_default_endpoint(base_url):
        raise CitationTransportError(
            f"{url_var}={base_url!r} names a non-OpenAI endpoint but "
            "CITATION_LLM_API_KEY is unset; refusing to send OPENAI_API_KEY "
            "there. Set CITATION_LLM_API_KEY for that endpoint (any placeholder "
            "such as 'not-needed' for a keyless self-hosted server), or unset "
            f"{url_var} to use api.openai.com"
        )
    shared = env.get("OPENAI_API_KEY") or None
    return CitationTransport(
        base_url=base_url,
        api_key=shared,
        key_source="OPENAI_API_KEY" if shared else "none",
    )
