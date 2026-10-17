"""Text-to-speech with optional LLM-based formulation.

Two-step pipeline used by the persistent-chat ``Speak`` button:

1. **Formulation** (auxiliary LLM) — strips markdown, summarizes code
   blocks, converts tables/lists into flowing prose so the TTS output
   sounds natural. Skipped for short or already-clean text.
2. **Synthesis** (TTS model) — generates MP3 via the user's configured
   TTS endpoint (OpenAI-compatible).

Model + endpoint resolution mirrors the dispatcher's per-user chain
(user > project > system). Results are LRU-cached in process so
clicking Play twice on the same message doesn't re-bill.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import logging
import os
import re
import uuid
from collections import OrderedDict
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Optional

import httpx
import yaml
from openai import AsyncOpenAI

from orchestrator.services.capability_credentials import (
    resolve_capability_credentials as _resolve_capability_credentials,
)
from orchestrator.services.family_matcher import detect_family
from orchestrator.services.usage_ledger import UsageEvent, UsageLedger

logger = logging.getLogger(__name__)


class TtsSynthesisError(RuntimeError):
    """Raised when a TTS model *is* configured but synthesis fails (missing
    key, upstream 5xx, timeout). Distinct from "no model configured" (which
    returns ``None``) so the endpoint can answer ``502`` for a real failure
    instead of the ``204`` "feature off" signal — i.e. the button surfaces an
    error instead of silently doing nothing.

    ``code`` classifies *actionable* failures so the UI can say something useful
    instead of a generic "synthesis failed": ``"payment_required"`` (the
    ElevenLabs free-tier / out-of-credit 402 — "this voice needs a paid plan"),
    ``"auth"`` (401/403 — bad provider key), ``"rate_limit"`` (429). Everything
    else stays ``"generic"``. The endpoint maps the code to an HTTP status +
    a machine-readable body the cockpit localizes.
    """

    def __init__(self, message: str = "", *, code: str = "generic"):
        super().__init__(message or code)
        self.code = code


# Upstream HTTP status → an actionable TtsSynthesisError code, or None for
# "generic / transient" (retry-worthy, no special message). Shared by the
# ElevenLabs and OpenAI synthesis paths so both classify failures the same way.
def _tts_error_code(status: Optional[int]) -> Optional[str]:
    if status == 402:
        return "payment_required"
    if status in (401, 403):
        return "auth"
    if status == 429:
        return "rate_limit"
    return None


# Cache caps. ~50 MP3s × ~200 KB each ≈ 10 MB. The cache is per-process
# and resets on orchestrator restart — TTS audio isn't worth persisting
# to disk.
_AUDIO_CACHE_MAX = 50
_FORMULATION_CACHE_MAX = 200
_PLAN_CACHE_MAX = 100

_audio_cache: "OrderedDict[str, bytes]" = OrderedDict()
_formulation_cache: "OrderedDict[str, str]" = OrderedDict()
_plan_cache: "OrderedDict[str, dict]" = OrderedDict()
_audio_lock = asyncio.Lock()
_formulation_lock = asyncio.Lock()
_plan_lock = asyncio.Lock()


# Voice defaults per language. OpenAI's TTS-1 voices are language-agnostic
# but tonally distinct; alloy/nova render German fine. Override per-thread
# later if the user wants a different voice.
DEFAULT_VOICE_EN = "alloy"
DEFAULT_VOICE_DE = "nova"

# Heuristic: skip the formulation LLM for short or markdown-free text.
_FORMULATION_MIN_LEN = 60
_MARKDOWN_HINTS = ("**", "```", "|", "# ", "## ", "### ", "- ", "* ", "1.", "[", "](")


# --- Auxiliary-LLM reasoning control ----------------------------------------
# The rewrite / chunk-planning stages call the user's *auxiliary* model, which
# for the homelab default (`gemma-4-moe`) is a hybrid *thinking* model. Left
# unset, the request inherits the endpoint's default thinking mode — and when
# that's ON the model reasons over the whole message before emitting its first
# rewritten token. That pre-token thinking pass is the dominant cost of
# time-to-first-audio (observed 4–45 s).
#
# These stages are mechanical (strip markdown, split at natural boundaries) —
# there is nothing to reason about — so we force thinking OFF whenever the aux
# family exposes a binary thinking toggle (gemma via vLLM's
# `chat_template_kwargs.enable_thinking`). We deliberately do NOT send the toggle
# to families without one: `chat_template_kwargs` is a vLLM extension a plain
# OpenAI endpoint rejects with a 400, which would knock the rewrite out entirely.
# The knob's single source of truth is the family's `reasoning` block in
# config/model_config_matrix.yaml — the same block the agent path reads (it
# forces thinking *on* there; here we force it *off*).
def _find_repo_root() -> Path:
    """Walk up from this file to the directory containing ``config/``."""
    anchor = Path(__file__).resolve().parent
    for _ in range(5):
        if (anchor / "config" / "model_config_matrix.yaml").is_file():
            return anchor
        anchor = anchor.parent
    return Path.cwd()  # Docker WORKDIR /app fallback


@lru_cache(maxsize=1)
def _model_config_matrix() -> dict:
    path = _find_repo_root() / "config" / "model_config_matrix.yaml"
    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = yaml.safe_load(fh) or {}
        return data if isinstance(data, dict) else {}
    except Exception:
        logger.warning("Could not load model_config_matrix.yaml at %s", path)
        return {}


def _set_nested(d: dict, dotted: str, value: Any) -> None:
    """Set ``d['a']['b'] = value`` for ``dotted='a.b'``, creating dicts."""
    node = d
    parts = dotted.split(".")
    for part in parts[:-1]:
        nxt = node.get(part)
        if not isinstance(nxt, dict):
            nxt = {}
            node[part] = nxt
        node = nxt
    node[parts[-1]] = value


def _clamp_effort(level: str, options: list[str]) -> Optional[str]:
    """Nearest supported effort for ``level`` from a family's ``options`` list, or
    ``None`` when it can't be mapped (skip injection rather than risk a 400 on an
    unsupported value)."""
    if level in options:
        return level
    alias = {"minimal": "low", "xhigh": "high"}.get(level)
    return alias if alias in options else None


def _aux_reasoning_body(model: str, level: Optional[str] = None) -> dict:
    """``extra_body`` controlling the aux model's reasoning for the rewrite stage.

    ``level`` is the user's read-aloud reasoning preference: ``None`` / ``"off"``
    (the default) keeps reasoning OFF — the rewrite is mechanical and that pre-token
    thinking pass is the dominant time-to-first-audio cost — while a real level
    (``"low"`` / ``"medium"`` / ``"high"`` / …) turns thinking on or sets the
    effort. Mirrors the agent's ``resolve_reasoning_plan`` but stays orchestrator-
    local (reads the same ``config/model_config_matrix.yaml`` reasoning block) and
    only emits parameters a family actually supports, so an unsupported field never
    reaches an endpoint that would 400 on it. ``{}`` is a safe no-op for
    ``chat.completions.create``."""
    if not model:
        return {}
    lvl = (level or "").strip().lower()
    is_off = lvl in ("", "off", "none", "false")
    fam = detect_family(model).family
    cap = (_model_config_matrix().get(fam) or {}).get("reasoning")
    if not isinstance(cap, dict):
        return {}
    method = cap.get("method")
    if method == "binary_toggle":
        # gemma via vLLM: flip enable_thinking. off → false (the fast default),
        # any requested level → true.
        param = cap.get("toggle_param", "chat_template_kwargs.enable_thinking")
        tmap = cap.get("toggle_map") or {"on": True, "off": False}
        body: dict = {}
        _set_nested(body, param, tmap.get("off" if is_off else "on", not is_off))
        return body
    if method == "effort_enum" and cap.get("delivery") == "native":
        options = [str(o).lower() for o in (cap.get("options") or [])]
        if is_off:
            # No universal "off" for an effort model — use an explicit low-reasoning
            # option when the family offers one, else inherit the endpoint default
            # (matches the prior behavior: we never forced these families off).
            for off_opt in ("none", "minimal"):
                if off_opt in options:
                    return {"reasoning_effort": off_opt}
            return {}
        chosen = _clamp_effort(lvl, options)
        return {"reasoning_effort": chosen} if chosen else {}
    # prompt-delivery effort, always_on, token_budget, none → nothing to inject.
    return {}


def _aux_reasoning_off_body(model: str) -> dict:
    """The reasoning-OFF ``extra_body`` (the default read-aloud path). Thin alias
    over :func:`_aux_reasoning_body` with no requested level."""
    return _aux_reasoning_body(model, None)


def _aux_family_extra_body(model: str) -> dict:
    """The aux family's static ``settings.extra_body`` from the model matrix —
    the SAME block the main agent applies via ``create_llm``. The load-bearing
    one is MiniMax's ``reasoning_split: true``, which tells MiniMax to return
    reasoning in a separate ``reasoning_content`` field instead of dumping
    ``<think>…</think>`` into ``content`` (where it would otherwise be rewritten
    and read aloud). The TTS rewrite path is a separate lane that historically
    only sent the reasoning toggle, so this closes the "extra_body layer 2" gap.
    Returns ``{}`` when the family declares none."""
    if not model:
        return {}
    fam = detect_family(model).family
    settings = (_model_config_matrix().get(fam) or {}).get("settings")
    extra = settings.get("extra_body") if isinstance(settings, dict) else None
    return dict(extra) if isinstance(extra, dict) else {}


def _aux_extra_body(model: str, level: Optional[str] = None) -> dict:
    """Full ``extra_body`` for a TTS aux call: the family's static anti-leak
    ``settings.extra_body`` (e.g. ``reasoning_split``) merged with the reasoning
    on/off toggle. The reasoning toggle wins on any key overlap (they're
    disjoint in practice: ``reasoning_split`` vs ``chat_template_kwargs``)."""
    return {**_aux_family_extra_body(model), **_aux_reasoning_body(model, level)}


# Belt-and-suspenders think-tag stripping. Even with reasoning_split (above),
# a leaky/unknown provider — or a future model — could embed its chain-of-thought
# as ``<think>…</think>`` in the content. That text must NEVER be spoken, so the
# rewrite path strips it structurally on top of suppressing it at the source.
_THINK_BLOCK_RE = re.compile(r"<think>.*?</think>", re.DOTALL | re.IGNORECASE)
_THINK_OPEN = "<think>"


def _withhold_partial_open(text: str) -> str:
    """Trim a trailing fragment of ``text`` that is a case-insensitive prefix of
    ``<think>`` — so a tag arriving split across stream deltas is never emitted
    half-formed (e.g. text ending in ``<thi``)."""
    lower = text.lower()
    for cut in range(len(_THINK_OPEN) - 1, 0, -1):
        if lower.endswith(_THINK_OPEN[:cut]):
            return text[:-cut]
    return text


def _strip_think_tags(text: str) -> str:
    """Remove ``<think>…</think>`` reasoning blocks from a fully-buffered string
    (the non-streaming rewrite paths). Also drops a dangling unclosed ``<think>``
    (reasoning cut off by a token cap) and any stray closer."""
    if not text:
        return text
    text = _THINK_BLOCK_RE.sub("", text)
    idx = text.lower().find(_THINK_OPEN)
    if idx != -1:
        text = text[:idx]
    text = re.sub(r"</think>", "", text, flags=re.IGNORECASE)
    return text.strip()


def _strip_think_stream(text: str, *, final: bool = False) -> str:
    """Streaming-safe strip: given the raw accumulation so far, return the prefix
    that is SAFE to emit now. Complete ``<think>…</think>`` blocks are removed; if
    reasoning is still open (an unclosed ``<think>``) everything from it is
    withheld until ``</think>`` arrives; and a tail that could be a ``<think>``
    tag mid-arrival is withheld. The caller diffs against what it already emitted
    to feed only the new clean content to the chunker. ``final=True`` (call once
    at end of stream) stops withholding the partial-tag tail — the stream ended,
    so a lingering ``<th…`` is real content, not a truncated tag."""
    text = _THINK_BLOCK_RE.sub("", text)
    open_idx = text.lower().rfind(_THINK_OPEN)
    if open_idx != -1:
        return text[:open_idx]
    return text if final else _withhold_partial_open(text)


# Valid read-aloud reasoning levels (Settings picker + server validation). "off"
# is the default and keeps the fast rewrite path; the rest turn the aux model's
# thinking on (or set its effort where the family supports levels).
READ_ALOUD_REASONING_LEVELS = ("off", "low", "medium", "high")
# The user's custom rewrite instructions ride on every aux call, so cap them to
# keep aux context + latency sane (mirrors the preview-text cap).
READ_ALOUD_PROMPT_MAX = 1000

# Appended to a rewrite system prompt when the user has set custom read-aloud
# instructions. Their preference outranks the default rules (they may ask to
# summarize / shorten / omit) — but NEVER the no-fabrication / no-altered-figures
# floor, because the whole point of read-aloud is to hear the message faithfully.
_READ_ALOUD_PREFERENCE_TEMPLATE = """The user has standing preferences for how their own messages are read aloud to them. Follow these preferences. Where they conflict with the rules above — for example the user may ask you to summarize, shorten, skip sections, or leave out particular details — THE USER'S PREFERENCES WIN. Two limits still hold regardless of the preferences: never invent facts that are not in the source text, and never change numbers, names, code identifiers, or quoted figures (you may drop them if asked, but never alter them).

User preferences:
{custom}"""


def _augment_rewrite_prompt(base: str, custom_prompt: Optional[str]) -> str:
    """Append the user's custom read-aloud instructions to a rewrite system prompt
    (no-op when unset). User preference outranks the default rules but not the
    no-fabrication floor — see :data:`_READ_ALOUD_PREFERENCE_TEMPLATE`."""
    custom = (custom_prompt or "").strip()
    if not custom:
        return base
    return f"{base}\n\n{_READ_ALOUD_PREFERENCE_TEMPLATE.format(custom=custom)}"


def _read_aloud_prefs(user_settings: dict) -> tuple[Optional[str], str]:
    """Extract ``(custom_prompt, reasoning_level)`` from the user's ``read_aloud``
    settings sub-object. Missing/blank → ``(None, "off")`` (the fast default)."""
    ra = user_settings.get("read_aloud") if isinstance(user_settings, dict) else None
    if not isinstance(ra, dict):
        return None, "off"
    custom = ra.get("custom_prompt")
    custom = custom.strip() if isinstance(custom, str) and custom.strip() else None
    level = ra.get("reasoning_level")
    level = level.strip().lower() if isinstance(level, str) and level.strip() else "off"
    if level not in READ_ALOUD_REASONING_LEVELS:
        level = "off"
    return custom, level


def _rewrite_variant_key(
    custom_prompt: Optional[str], reasoning_level: Optional[str]
) -> str:
    """A short, stable cache discriminator for the rewrite variant. The rewritten
    text depends on the user's custom instructions and reasoning level, so the
    plan / formulation caches (keyed on the ORIGINAL content) must fold it in — or
    editing the read-aloud prompt would replay a stale rewrite from cache."""
    cp = (custom_prompt or "").strip()
    lvl = (reasoning_level or "off").strip().lower() or "off"
    return _hash_key("rw", lvl, cp)


FORMULATION_SYSTEM_PROMPT = """You rewrite text so it sounds natural when read aloud by a text-to-speech engine.

Rules:
1. Strip ALL markdown (asterisks, headers, code fences, link syntax).
2. Convert tables to descriptive sentences — never read tables cell-by-cell.
3. For code blocks: briefly describe what the code does in one sentence; never read syntax.
4. Convert bullet lists to flowing prose with words like "first", "next", "also".
5. Drop URLs (or say "a link").
6. Preserve the meaning, tone, technical terms, and proper names.
7. Keep numbers in a readable form.

Return ONLY the rewritten text, no preamble, no commentary."""

# Long messages are read aloud as a sequence of separately-synthesized chunks.
# TTS_CHUNK_LIMIT is the API's hard input cap (OpenAI rejects >4096); the gate
# never exceeds it. TTS_CHUNK_TARGET is the size we actually aim chunks at, kept
# small because a CPU TTS model synthesizes at ~real-time (~38 chars/s measured
# for kokoro-cpu): ~1500 chars ≈ ~40 s of synthesis — fast first audio, inside
# the per-chunk timeout, and short enough to not trip the backend liveness probe.
# (Faster backends could use a larger target; backend-aware sizing is a TODO.)
TTS_CHUNK_LIMIT = 4096
TTS_CHUNK_TARGET = 1500
# The first chunk gates when the *first* audio is heard, so keep it small — a
# short chunk 1 synthesizes fast and playback can start while the rest generate
# (time-to-first-audio beats any progress bar). Enforced deterministically by
# _shorten_first_chunk regardless of what the LLM returns.
TTS_FIRST_CHUNK_TARGET = 500

CHUNKING_SYSTEM_PROMPT = f"""You rewrite text so it sounds natural read aloud by a text-to-speech engine, AND split it into ordered chunks for sequential synthesis.

Rewrite rules:
1. Strip ALL markdown (asterisks, headers, code fences, link syntax).
2. Convert tables to descriptive sentences — never read tables cell-by-cell.
3. For code blocks: briefly describe what the code does in one sentence; never read syntax.
4. Convert bullet lists to flowing prose with words like "first", "next", "also".
5. Drop URLs (or say "a link").
6. Preserve the meaning, tone, technical terms, proper names, and ORDER. Do not summarize or omit content.
7. Keep numbers in a readable form.

Chunking rules:
8. Split the rewritten text into chunks of at most {TTS_CHUNK_TARGET} characters each (hard ceiling {TTS_CHUNK_LIMIT}).
9. Break ONLY at natural stopping points — the end of a section, paragraph, or complete thought — so each chunk ends on a natural pause and the audio never sounds cut off mid-idea. Never split mid-sentence.
10. Make the FIRST chunk short — about {TTS_FIRST_CHUNK_TARGET} characters — so audio can start quickly; the remaining chunks may run up to the {TTS_CHUNK_TARGET}-character target.
11. Short input is a single chunk; long input becomes as many chunks as needed, in order.

Return ONLY a JSON array of strings (the chunks, in order), e.g. ["first chunk", "second chunk"]. No preamble, no commentary, no markdown fences."""

# Streaming chunk plan. The auxiliary LLM streams the *cleaned* text token by
# token; the client synthesizes + plays chunk 1 while the rest still generate
# (time-to-first-audio ≈ first-chunk latency, not whole-message latency). This is
# what retires the old "one 30 s+ call that times out" failure mode: there is no
# single long call, only a trickle bounded by a per-token idle timeout.
#
# CHUNKING IS DONE HERE, NOT BY THE MODEL. Measured behaviour: gemma-4-moe (and
# likely other local models) reliably *rewrites* but ignores an instruction to
# emit an explicit chunk separator — so we can't depend on one. Instead we flush
# a chunk whenever the streamed buffer crosses the size target at a natural
# sentence/paragraph boundary. The sentinel below is honoured as a *bonus*
# boundary when a model does emit it, but nothing depends on it.
TTS_CHUNK_SENTINEL = "[[BREAK]]"
# Two-tier stream timeout. Before the FIRST token, be patient — first-token
# latency on a loaded/cold homelab endpoint is highly variable (measured 4–45 s)
# and the user has explicitly said they'll wait for a good rewrite (and can hit
# the "read it as-is" bailout in the UI if they won't). Once tokens are FLOWING,
# a long gap really does mean a dead connection, so tighten up. Neither is a
# total-request cap — a long message that keeps trickling is fine.
_STREAM_FIRST_TOKEN_TIMEOUT = 120.0
_STREAM_IDLE_TIMEOUT = 30.0
# Generous overall ceiling so a totally stuck (but not idle) connection still dies.
_STREAM_MAX_TOTAL = 600.0

CHUNKING_STREAM_SYSTEM_PROMPT = """You rewrite text so it sounds natural read aloud by a text-to-speech engine, streaming the rewritten text as you go.

Rules:
1. Strip ALL markdown (asterisks, headers, code fences, link syntax).
2. Convert tables to descriptive sentences — never read tables cell-by-cell.
3. For code blocks: briefly describe what the code does in one sentence; never read syntax.
4. Convert bullet lists to flowing prose with words like "first", "next", "also".
5. Drop URLs (or say "a link").
6. Preserve the meaning, tone, technical terms, proper names, and ORDER. Do not summarize or omit content.
7. Keep numbers in a readable form.
8. Separate distinct sections or ideas with a blank line so the reader can pause naturally.

Output ONLY the rewritten spoken text, streamed as you go. No JSON, no markdown, no preamble, no commentary, no numbering."""


def _hash_key(*parts: str) -> str:
    """Stable cache key from any string parts."""
    h = hashlib.sha256()
    for p in parts:
        h.update(p.encode("utf-8"))
        h.update(b"\x00")
    return h.hexdigest()[:32]


def _cache_get(cache: OrderedDict, key: str) -> Any:
    val = cache.get(key)
    if val is not None:
        cache.move_to_end(key)
    return val


def _cache_put(cache: OrderedDict, key: str, value: Any, max_size: int) -> None:
    cache[key] = value
    cache.move_to_end(key)
    while len(cache) > max_size:
        cache.popitem(last=False)


# ---------------------------------------------------------------------------
# Usage metering (rate-limiting v2). Read-aloud calls resolved endpoints
# directly (no gateway), so synthesis AND the auxiliary formulation/chunking
# LLM calls are invisible to ``usage_events`` unless we emit here. Metering is
# **non-load-bearing**: a missing ledger (audit pool absent) or a write error
# never affects playback. Gated on ``_metering_enabled`` so the hot path skips
# all work — and the mocked ``response.usage`` in unit tests is never touched —
# when no ledger is wired (ledger is ``None`` for every direct-call test).
# ---------------------------------------------------------------------------


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _metering_enabled(ledger: Optional[UsageLedger]) -> bool:
    return ledger is not None and getattr(ledger, "is_available", False)


async def _record_usage(
    ledger: Optional[UsageLedger], events: list[UsageEvent]
) -> None:
    if not events:
        return
    try:
        await ledger.record_events(events)
    except Exception:
        logger.debug("voice usage metering failed (non-fatal)", exc_info=True)


def _token_events_from_usage(
    usage: Any,
    *,
    model: str,
    stage: str,
    user_id: Optional[str],
    ref_id: Optional[str],
) -> list[UsageEvent]:
    """Prompt/completion-token ``UsageEvent``s from an OpenAI-compatible ``usage``
    object — ``[]`` when it's ``None`` (many local servers omit usage). Emitted
    under ``source='orchestrator'`` so they stay attributable to the TTS aux
    without colliding with chat usage rows. Shared by the
    buffered (``response.usage``) and streaming (final-chunk ``usage``) paths."""
    if usage is None:
        return []
    common = dict(
        category="llm",
        resource=model,
        source="orchestrator",
        source_id=uuid.uuid4().hex,
        ts=_now(),
        user_id=user_id,
        ref_kind="thread",
        ref_id=ref_id,
        details={"via": "tts", "stage": stage},
    )
    events: list[UsageEvent] = []
    prompt_tokens = getattr(usage, "prompt_tokens", None)
    completion_tokens = getattr(usage, "completion_tokens", None)
    if prompt_tokens:
        events.append(
            UsageEvent(quantity=int(prompt_tokens), unit="prompt-token", **common)
        )
    if completion_tokens:
        events.append(
            UsageEvent(
                quantity=int(completion_tokens), unit="completion-token", **common
            )
        )
    return events


def _llm_token_events(
    response: Any,
    *,
    model: str,
    stage: str,
    user_id: Optional[str],
    ref_id: Optional[str],
) -> list[UsageEvent]:
    """Token ``UsageEvent``s from a buffered chat response (``response.usage``)."""
    return _token_events_from_usage(
        getattr(response, "usage", None),
        model=model,
        stage=stage,
        user_id=user_id,
        ref_id=ref_id,
    )


def _voice_for_language(language: str) -> str:
    if language and language.lower().startswith("de"):
        return DEFAULT_VOICE_DE
    return DEFAULT_VOICE_EN


# Cheap EN/DE content-language sniff (the app supports en + de-DE). It exists
# only so German text stops being read aloud in an English voice; a fuller
# detector is future work. Umlauts/ß are the strong signal; a few common German
# function words catch umlaut-free German.
_GERMAN_HINT_CHARS = frozenset("äöüßÄÖÜ")
_GERMAN_HINT_WORDS = frozenset(
    "der die das und ist nicht ein eine mit auf für sich dass werden wird auch "
    "dem den von zu im ich du wir ihr sie oder aber wenn weil".split()
)


def _detect_language(text: str) -> str:
    """Best-effort content language for voice selection: ``'de'`` or ``'en'``."""
    if not text:
        return "en"
    if any(c in _GERMAN_HINT_CHARS for c in text):
        return "de"
    words = re.findall(r"[a-zA-Zäöüß]+", text.lower())
    if not words:
        return "en"
    hits = sum(1 for w in words if w in _GERMAN_HINT_WORDS)
    return "de" if hits >= max(2, len(words) // 20) else "en"


def _pick_voice(params: dict, language: str, user_voice: Optional[str]) -> str:
    """Voice resolution priority (knowledge-base/knowledge/features/voice_experience_roadmap.md):
    explicit user choice (``default_tts_voice``) → admin single ``voice`` →
    admin per-language ``voices`` map → built-in per-language default.

    Different backends expose different voice names (Kokoro ``af_*``, OpenAI
    ``alloy``/``nova``), so there is no one hardcoded name — the per-language map
    lets one TTS model serve both languages with a language-appropriate voice.
    """
    if user_voice:
        return user_voice
    single = (params.get("voice") or "").strip()
    if single:
        return single
    voices = params.get("voices")
    if isinstance(voices, dict):
        base = language.split("-")[0] if language else ""
        for key in (language, base):
            if key:
                v = (voices.get(key) or "").strip()
                if v:
                    return v
    return _voice_for_language(language)


def _needs_formulation(text: str) -> bool:
    """Cheap markdown sniffer to skip the formulation LLM when not useful."""
    if not text or len(text) < _FORMULATION_MIN_LEN:
        return False
    return any(hint in text for hint in _MARKDOWN_HINTS)


async def _formulate_for_speech(
    text: str,
    *,
    model: str,
    base_url: Optional[str],
    api_key: Optional[str],
    timeout: float = 30.0,
    ledger: Optional[UsageLedger] = None,
    user_id: Optional[str] = None,
    ref_id: Optional[str] = None,
    custom_prompt: Optional[str] = None,
    reasoning_level: Optional[str] = None,
) -> str:
    """Run the auxiliary LLM to rewrite ``text`` for natural speech.

    ``custom_prompt`` (the user's standing read-aloud instructions) and
    ``reasoning_level`` (off by default) come from the user's ``read_aloud``
    settings; both feed the cache key so a preference change doesn't replay a
    stale rewrite."""
    cache_key = _hash_key(
        model, _rewrite_variant_key(custom_prompt, reasoning_level), text
    )
    async with _formulation_lock:
        cached = _cache_get(_formulation_cache, cache_key)
    if cached is not None:
        logger.debug("TTS formulation cache hit (%s chars)", len(text))
        return cached

    if not api_key:
        logger.warning(
            "TTS formulation skipped: no API key (stage=formulate model=%s base_url=%s)",
            model,
            base_url,
        )
        return text

    # max_retries=0: a down/erroring endpoint must fail in seconds, not retry
    # with backoff for minutes — the "Read aloud" button would otherwise appear
    # to hang. (A 503 here previously stretched a single click to ~213s.)
    client = AsyncOpenAI(
        api_key=api_key, base_url=base_url, timeout=timeout, max_retries=0
    )
    try:
        response = await client.chat.completions.create(
            model=model,
            messages=[
                {
                    "role": "system",
                    "content": _augment_rewrite_prompt(
                        FORMULATION_SYSTEM_PROMPT, custom_prompt
                    ),
                },
                {"role": "user", "content": text},
            ],
            temperature=0.3,
            extra_body=_aux_extra_body(model, reasoning_level),
        )
        rewritten = _strip_think_tags(response.choices[0].message.content or "")
        if _metering_enabled(ledger):
            await _record_usage(
                ledger,
                _llm_token_events(
                    response,
                    model=model,
                    stage="formulate",
                    user_id=user_id,
                    ref_id=ref_id,
                ),
            )
    except Exception:
        logger.exception(
            "TTS formulation LLM call failed (stage=formulate model=%s base_url=%s); "
            "falling back to raw text",
            model,
            base_url,
        )
        return text
    finally:
        await client.close()

    if not rewritten:
        return text

    async with _formulation_lock:
        _cache_put(_formulation_cache, cache_key, rewritten, _FORMULATION_CACHE_MAX)
    logger.info("TTS formulation: %d → %d chars", len(text), len(rewritten))
    return rewritten


# ElevenLabs REST TTS (not OpenAI-compatible — own header/path/model ids). See
# knowledge-base/knowledge/features/tts_vendor_providers.md. mp3_44100_128 matches the mp3 the
# OpenAI path returns, so the cache/metering/player upstream stay format-blind.
_ELEVENLABS_BASE = "https://api.elevenlabs.io"
_ELEVENLABS_OUTPUT_FORMAT = "mp3_44100_128"


def _resolve_tts_provider(model_id: str, provider: Optional[str]) -> str:
    """Which synthesis backend a TTS model speaks. An explicit
    ``params_json.provider`` wins; otherwise sniff the model id (ElevenLabs
    model ids are ``eleven_*``). Defaults to ``"openai"`` — the OpenAI-
    compatible shape that Kokoro / OpenAI / Groq all share."""
    explicit = (provider or "").strip().lower()
    if explicit:
        return explicit
    if "eleven" in (model_id or "").lower():
        return "elevenlabs"
    return "openai"


async def _synthesize_elevenlabs(
    text: str,
    *,
    model: str,
    voice: str,
    api_key: Optional[str],
    timeout: float = 120.0,
) -> Optional[bytes]:
    """Synthesize MP3 via the ElevenLabs REST API.

    Auth is ``xi-api-key``; the voice is an opaque account ``voice_id`` in the
    path; ``model_id`` is ElevenLabs' own (``eleven_multilingual_v2`` etc.,
    carried as the registry row's ``model_id``). The key is the resolved
    credential if the row carries one, else the deployment-wide
    ``ELEVENLABS_API_KEY`` (one account per deployment). Same no-retry /
    fail-in-seconds posture as :func:`_synthesize_speech` so the "Read aloud"
    button never hangs. Returns ``None`` on a generic/transient failure (the
    caller maps that to a 502); RAISES :class:`TtsSynthesisError` with a ``code``
    on an *actionable* upstream error (402 needs-paid-plan, 401 auth, 429 rate
    limit) so the UI can say what actually went wrong — the exact case where a
    free-tier account picks a Library voice it can't synthesize."""
    key = (api_key or os.environ.get("ELEVENLABS_API_KEY") or "").strip()
    if not key:
        logger.warning(
            "ElevenLabs synthesis aborted: no ELEVENLABS_API_KEY (model=%s)", model
        )
        return None
    if not voice:
        # ElevenLabs has no built-in default voice — the registry row's
        # params_json must set one (a voice_id). Fail loud rather than 404.
        logger.warning(
            "ElevenLabs synthesis aborted: no voice id resolved (model=%s)", model
        )
        return None
    url = f"{_ELEVENLABS_BASE}/v1/text-to-speech/{voice}"
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.post(
                url,
                params={"output_format": _ELEVENLABS_OUTPUT_FORMAT},
                headers={"xi-api-key": key, "accept": "audio/mpeg"},
                json={"text": text, "model_id": model},
            )
    except Exception:
        logger.exception(
            "ElevenLabs synthesis request failed (model=%s voice=%s chars=%d)",
            model,
            voice,
            len(text),
        )
        return None

    if resp.status_code < 400:
        return resp.content

    code = _tts_error_code(resp.status_code)
    message = _elevenlabs_error_message(resp)
    logger.warning(
        "ElevenLabs synthesis rejected (status=%s code=%s voice=%s): %s",
        resp.status_code,
        code,
        voice,
        message,
    )
    if code:
        raise TtsSynthesisError(message, code=code)
    return None


# ElevenLabs account-voice listing (Phase 5 — settings voice picker). The
# deployment account's voice list changes rarely and listing is free (spends no
# characters), so cache it in-process ~5 min: opening Settings shouldn't hit
# ElevenLabs on every render. Keyed by the api key so a rotated key re-fetches.
_VOICES_CACHE_TTL_SECONDS = 300
_voices_cache: "dict[str, tuple[datetime, list[dict]]]" = {}
_voices_lock = asyncio.Lock()


def _map_elevenlabs_voice(v: dict) -> dict:
    """One ``GET /v2/voices`` entry → the picker's ``{id, name, labels,
    preview_url}``. ``labels`` (accent / gender / age / description) is
    ElevenLabs' own metadata — strictly better than prefix-decoding — and the
    cockpit composes option labels from it. ``preview_url`` is a public CDN mp3
    the browser can hotlink to audition the voice without spending characters."""
    labels = v.get("labels")
    return {
        "id": v.get("voice_id") or "",
        "name": v.get("name") or v.get("voice_id") or "",
        "labels": labels if isinstance(labels, dict) else {},
        "preview_url": v.get("preview_url") or None,
    }


async def _fetch_elevenlabs_voices(api_key: str, timeout: float = 15.0) -> list[dict]:
    """Live ``GET /v2/voices`` → mapped picker entries. Raises on HTTP error so
    the caller can log-and-degrade rather than cache a failure."""
    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.get(
            f"{_ELEVENLABS_BASE}/v2/voices",
            headers={"xi-api-key": api_key, "accept": "application/json"},
        )
        resp.raise_for_status()
        data = resp.json()
    voices = data.get("voices") if isinstance(data, dict) else None
    if not isinstance(voices, list):
        return []
    return [_map_elevenlabs_voice(v) for v in voices if isinstance(v, dict)]


async def invalidate_account_voices_cache() -> None:
    """Drop the cached account voice list so a freshly-added/designed voice
    (Phases 6/7) shows up immediately instead of after the TTL."""
    async with _voices_lock:
        _voices_cache.clear()


async def _resolve_elevenlabs_context(
    *, user_id: str, postgres_db
) -> tuple[Optional[str], Optional[str]]:
    """Resolve the caller's configured TTS backend and — for ElevenLabs — the
    deployment api key. Returns ``(backend, key)``:

    - ``backend`` is ``None`` when no TTS model is configured, else the resolved
      backend name (``"kokoro"`` / ``"openai"`` / ``"elevenlabs"`` / …).
    - ``key`` is the ElevenLabs api key (resolved credential, else the
      deployment-wide ``ELEVENLABS_API_KEY``) when the backend is ElevenLabs and
      a key exists; ``None`` for non-ElevenLabs backends or a missing key.

    Shared by the account-voice list and the Voice Library proxies so all three
    agree on which ElevenLabs account (and key) they're talking to."""
    user_settings = await postgres_db.get_user_settings(user_id) or {}
    resolved_keys = await postgres_db.resolve_api_keys_for_job(
        user_id=user_id, project_id=None
    )
    tts_creds = await _resolve_capability_credentials(
        capability="tts",
        user_settings=user_settings,
        user_id=user_id,
        resolved_keys=resolved_keys,
        postgres_db=postgres_db,
    )
    if tts_creds is None:
        return None, None
    backend = _resolve_tts_provider(tts_creds.wire_model, tts_creds.provider)
    if backend != "elevenlabs":
        return backend, None
    key = (tts_creds.api_key or os.environ.get("ELEVENLABS_API_KEY") or "").strip()
    return backend, (key or None)


async def list_account_voices(*, user_id: str, postgres_db) -> dict:
    """Voices the user's configured TTS backend offers, for the Settings picker.

    Returns ``{"backend": <str|None>, "voices": [{id, name, labels,
    preview_url}]}``. Only ElevenLabs returns a populated list (fetched live
    from the one-per-deployment account, cached ~5 min); Kokoro/OpenAI return
    ``voices: []`` because the cockpit holds their static catalogs locally. No
    TTS model configured → ``backend: None``. A listing failure degrades to an
    empty list (never raises) so Settings still renders — the voice field just
    falls back to free-text entry, same as an unrecognized backend."""
    backend, key = await _resolve_elevenlabs_context(
        user_id=user_id, postgres_db=postgres_db
    )
    if backend != "elevenlabs":
        # None (no model) or a static-catalog backend (Kokoro / OpenAI) — the
        # cockpit already holds their voice lists locally.
        return {"backend": backend, "voices": []}
    if not key:
        logger.warning("ElevenLabs voice list: no ELEVENLABS_API_KEY resolved")
        return {"backend": backend, "voices": []}

    cache_key = _hash_key("el-voices", key)
    async with _voices_lock:
        hit = _voices_cache.get(cache_key)
        if (
            hit is not None
            and (_now() - hit[0]).total_seconds() < _VOICES_CACHE_TTL_SECONDS
        ):
            return {"backend": backend, "voices": hit[1]}

    try:
        voices = await _fetch_elevenlabs_voices(key)
    except Exception:
        logger.exception("ElevenLabs voice list fetch failed")
        return {"backend": backend, "voices": []}

    async with _voices_lock:
        _voices_cache[cache_key] = (_now(), voices)
    return {"backend": backend, "voices": voices}


# ElevenLabs Voice Library (Phase 6 — browse the 10k+ community library and add
# voices to the deployment account). Browsing/previewing is read-only and free
# (public CDN previews); *adding* copies a voice into the shared account and
# consumes a plan-limited voice slot, so the endpoint that calls
# :func:`add_library_voice` gates it behind an admin flag.
_LIBRARY_PAGE_SIZE = 30


class TtsLibraryError(RuntimeError):
    """A Voice Library mutation (add) failed in a way worth showing the user —
    most importantly the account's plan voice-slot limit. Carries a readable
    ``message`` and an HTTP ``status_code`` so the endpoint surfaces it instead
    of leaking a bare 500."""

    def __init__(self, message: str, *, status_code: int = 502):
        super().__init__(message)
        self.message = message
        self.status_code = status_code


def _map_shared_voice(v: dict) -> dict:
    """One ``GET /v1/shared-voices`` entry → a library result card. The shared
    library exposes accent / gender / age / language as flat fields (not the
    account voices' ``labels`` dict), plus ``public_owner_id`` — the pair
    ``(public_owner_id, voice_id)`` is what ``/v1/voices/add`` needs to copy the
    voice into the deployment account."""

    def _s(*keys: str) -> Optional[str]:
        for k in keys:
            val = v.get(k)
            if isinstance(val, str) and val:
                return val
        return None

    return {
        "id": v.get("voice_id") or "",
        "public_owner_id": v.get("public_owner_id") or "",
        "name": v.get("name") or v.get("voice_id") or "",
        "accent": _s("accent"),
        "gender": _s("gender"),
        "age": _s("age"),
        "language": _s("language"),
        "description": _s("description", "descriptive"),
        "preview_url": v.get("preview_url") or None,
        "free": bool(v.get("free_users_allowed")),
    }


async def _fetch_shared_voices(
    api_key: str, params: dict, timeout: float = 15.0
) -> tuple[list[dict], bool]:
    """Live ``GET /v1/shared-voices`` → ``(mapped cards, has_more)``. Raises on
    HTTP error so the caller can degrade with a readable message."""
    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.get(
            f"{_ELEVENLABS_BASE}/v1/shared-voices",
            headers={"xi-api-key": api_key, "accept": "application/json"},
            params=params,
        )
        resp.raise_for_status()
        data = resp.json()
    if not isinstance(data, dict):
        return [], False
    voices = data.get("voices")
    has_more = bool(data.get("has_more"))
    if not isinstance(voices, list):
        return [], has_more
    return [_map_shared_voice(v) for v in voices if isinstance(v, dict)], has_more


async def search_voice_library(*, user_id: str, postgres_db, filters: dict) -> dict:
    """Search the ElevenLabs community Voice Library (read-only proxy).

    ``filters`` carries optional ``search`` / ``language`` / ``accent`` /
    ``gender`` / ``age`` / ``page`` passthroughs. Returns ``{"backend",
    "voices": [...], "has_more": bool, "error": <str|None>}``. Non-ElevenLabs
    backends (or no TTS model) return an empty list — the library is
    ElevenLabs-only. A fetch failure degrades to ``voices: []`` with a readable
    ``error`` rather than a 5xx, so the browser shows a banner, not a crash."""
    backend, key = await _resolve_elevenlabs_context(
        user_id=user_id, postgres_db=postgres_db
    )
    base = {"backend": backend, "voices": [], "has_more": False, "error": None}
    if backend != "elevenlabs":
        return base
    if not key:
        return {**base, "error": "ElevenLabs is not configured."}

    params: dict = {"page_size": _LIBRARY_PAGE_SIZE}
    for name in ("search", "language", "accent", "gender", "age"):
        raw = filters.get(name)
        val = raw.strip() if isinstance(raw, str) else raw
        if val:
            params[name] = val
    try:
        page = int(filters.get("page") or 0)
    except (TypeError, ValueError):
        page = 0
    if page > 0:
        params["page"] = page

    try:
        voices, has_more = await _fetch_shared_voices(key, params)
    except Exception:
        logger.exception("ElevenLabs voice library search failed")
        return {**base, "error": "Voice library search failed. Try again."}
    return {"backend": backend, "voices": voices, "has_more": has_more, "error": None}


def _elevenlabs_error_message(resp) -> str:
    """Pull a human-readable message out of an ElevenLabs error response. Their
    errors are ``{"detail": {"status": ..., "message": ...}}`` or ``{"detail":
    "..."}``; fall back to a generic line."""
    try:
        body = resp.json()
    except Exception:
        body = None
    if isinstance(body, dict):
        detail = body.get("detail")
        if isinstance(detail, dict):
            msg = detail.get("message") or detail.get("status")
            if isinstance(msg, str) and msg:
                return msg
        elif isinstance(detail, str) and detail:
            return detail
    return f"ElevenLabs returned an error (HTTP {resp.status_code})."


async def add_library_voice(
    *, user_id: str, postgres_db, public_owner_id: str, voice_id: str, new_name: str
) -> dict:
    """Copy a Library voice into the deployment ElevenLabs account.

    Proxies ``POST /v1/voices/add/{public_owner_id}/{voice_id}`` (body
    ``{new_name}``); on success invalidates the account-voice cache so the new
    voice shows up in the Settings picker immediately. Raises
    :class:`TtsLibraryError` (readable message + status) on any failure — the
    important one being the account's plan voice-slot limit, which ElevenLabs
    returns as a 4xx we translate rather than leak as a 500. Returns
    ``{"voice_id": <account-scoped id>, "name": <new_name>}``."""
    backend, key = await _resolve_elevenlabs_context(
        user_id=user_id, postgres_db=postgres_db
    )
    if backend != "elevenlabs":
        raise TtsLibraryError(
            "ElevenLabs is not the configured TTS provider.", status_code=400
        )
    if not key:
        raise TtsLibraryError("ElevenLabs is not configured.", status_code=400)

    url = f"{_ELEVENLABS_BASE}/v1/voices/add/{public_owner_id}/{voice_id}"
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                url,
                headers={"xi-api-key": key, "accept": "application/json"},
                json={"new_name": new_name},
            )
    except Exception as exc:
        logger.exception("ElevenLabs add-voice request failed")
        raise TtsLibraryError("Could not reach ElevenLabs. Try again.") from exc

    if resp.status_code >= 400:
        message = _elevenlabs_error_message(resp)
        logger.warning(
            "ElevenLabs add-voice rejected (status=%s): %s", resp.status_code, message
        )
        # A 4xx from ElevenLabs is a client-actionable condition (slot limit, bad
        # id) — surface its status so the UI distinguishes "your fault" from 502.
        status = resp.status_code if 400 <= resp.status_code < 500 else 502
        raise TtsLibraryError(message, status_code=status)

    try:
        data = resp.json()
    except Exception:
        data = {}
    new_id = data.get("voice_id") if isinstance(data, dict) else None
    await invalidate_account_voices_cache()
    return {"voice_id": new_id or "", "name": new_name}


async def _synthesize_speech(
    text: str,
    *,
    model: str,
    voice: str,
    base_url: Optional[str],
    api_key: Optional[str],
    timeout: float = 120.0,
    instructions: Optional[str] = None,
    provider: Optional[str] = None,
) -> Optional[bytes]:
    """Call the TTS endpoint and return MP3 bytes.

    Timeout is generous (120 s) because a CPU TTS model synthesizes at
    ~real-time: a ~1500-char chunk is ~40 s, and we want headroom for a slower
    chunk or a loaded backend rather than a spurious failure.

    ``instructions`` is a free-text style prompt (e.g. "warm, unhurried") passed
    through to instruction-capable models like OpenAI ``gpt-4o-mini-tts`` — the
    persona hook for Phase 5. Only sent when set, since ``tts-1``/Kokoro reject
    the param.

    ``provider`` (from the row's ``params_json.provider``) selects the backend.
    ElevenLabs is not OpenAI-compatible, so it forks to :func:`_synthesize_
    elevenlabs`; everything else takes the OpenAI-compatible path below. The
    fork happens *before* the ``api_key`` guard because ElevenLabs supplies its
    key from the environment when the row carries none.
    """
    if _resolve_tts_provider(model, provider) == "elevenlabs":
        return await _synthesize_elevenlabs(
            text, model=model, voice=voice, api_key=api_key, timeout=timeout
        )

    if not api_key:
        logger.warning(
            "TTS synthesis aborted: no API key (stage=synthesize model=%s base_url=%s)",
            model,
            base_url,
        )
        return None

    # max_retries=0: a down/erroring endpoint must fail in seconds, not retry
    # with backoff for minutes — the "Read aloud" button would otherwise appear
    # to hang. (A 503 here previously stretched a single click to ~213s.)
    client = AsyncOpenAI(
        api_key=api_key, base_url=base_url, timeout=timeout, max_retries=0
    )
    extra = {"instructions": instructions} if instructions else {}
    try:
        response = await client.audio.speech.create(
            model=model,
            voice=voice,
            input=text,
            response_format="mp3",
            **extra,
        )
        return response.content
    except Exception as exc:
        logger.exception(
            "TTS synthesis failed (stage=synthesize model=%s base_url=%s voice=%s chars=%d)",
            model,
            base_url,
            voice,
            len(text),
        )
        # Classify actionable upstream errors (bad key, quota/rate limit) so the
        # UI can say what's wrong; the OpenAI SDK exposes the HTTP status.
        code = _tts_error_code(getattr(exc, "status_code", None))
        if code:
            raise TtsSynthesisError(str(exc), code=code) from exc
        return None
    finally:
        await client.close()


async def generate_message_tts(
    *,
    content: str,
    language: str,
    reformulate: bool,
    user_id: str,
    postgres_db,
    ledger: Optional[UsageLedger] = None,
    ref_id: Optional[str] = None,
    project_id: Optional[str] = None,
) -> Optional[tuple[str, bytes]]:
    """End-to-end: formulate (optional) → synthesize → ``(spoken_text, mp3)``.

    Returns the **spoken text** (the formulation-rewritten version actually
    sent to the TTS model, or the raw content when formulation was skipped)
    alongside the MP3 bytes, so the UI can show what was read aloud.

    Returns ``None`` when no TTS model is configured (the endpoint maps this to
    ``204``). Raises :class:`TtsSynthesisError` when a model *is* configured but
    synthesis fails, so the endpoint can answer ``502`` rather than silently
    no-op'ing. Errors during *formulation* remain non-fatal — synthesis falls
    back to the raw text in that case.
    """
    if not content or not content.strip():
        return None

    # Resolve user settings + API keys once. Both auxiliary and TTS draw
    # from the same pools.
    user_settings = await postgres_db.get_user_settings(user_id) or {}
    resolved_keys = await postgres_db.resolve_api_keys_for_job(
        user_id=user_id, project_id=None
    )

    tts_creds = await _resolve_capability_credentials(
        capability="tts",
        user_settings=user_settings,
        user_id=user_id,
        resolved_keys=resolved_keys,
        postgres_db=postgres_db,
    )
    if tts_creds is None:
        logger.info("No TTS model configured for user %s", user_id)
        return None
    tts_model = tts_creds.wire_model
    # Voice picked from the *content's* language (not the UI language) with the
    # user's explicit choice and admin per-language map layered on; ``language``
    # from the request is a fallback hint only. ``instructions`` is the style
    # prompt for instruction-capable models (gpt-4o-mini-tts).
    tts_params = tts_creds.params
    detected_language = _detect_language(content) or language
    user_voice = (user_settings.get("default_tts_voice") or "").strip() or None
    voice = _pick_voice(tts_params, detected_language, user_voice)
    instructions = (tts_params.get("instructions") or "").strip() or None

    speech_input = content
    if reformulate and _needs_formulation(content):
        aux_creds = await _resolve_capability_credentials(
            capability="auxiliary",
            user_settings=user_settings,
            user_id=user_id,
            resolved_keys=resolved_keys,
            postgres_db=postgres_db,
        )
        if aux_creds is not None:
            custom_prompt, reasoning_level = _read_aloud_prefs(user_settings)
            speech_input = await _formulate_for_speech(
                content,
                model=aux_creds.wire_model,
                base_url=aux_creds.base_url,
                api_key=aux_creds.api_key,
                ledger=ledger,
                user_id=user_id,
                ref_id=ref_id,
                custom_prompt=custom_prompt,
                reasoning_level=reasoning_level,
            )
        else:
            logger.debug(
                "Reformulation requested but no auxiliary model configured; "
                "synthesizing raw content"
            )

    audio_key = _hash_key(tts_model, voice, instructions or "", speech_input)
    async with _audio_lock:
        cached = _cache_get(_audio_cache, audio_key)
    if cached is not None:
        logger.debug("TTS audio cache hit")
        return speech_input, cached

    audio = await _synthesize_speech(
        speech_input,
        model=tts_model,
        voice=voice,
        base_url=tts_creds.base_url,
        api_key=tts_creds.api_key,
        instructions=instructions,
        provider=tts_params.get("provider"),
    )
    if not audio:
        # Model is configured but synthesis produced nothing (missing key,
        # upstream error, timeout). Raise so the endpoint returns 502 — a 204
        # here would be indistinguishable from "TTS not configured" and the
        # button would silently do nothing.
        raise TtsSynthesisError(f"TTS synthesis failed for model {tts_model}")
    async with _audio_lock:
        _cache_put(_audio_cache, audio_key, audio, _AUDIO_CACHE_MAX)
    if _metering_enabled(ledger):
        await _record_usage(
            ledger,
            [
                UsageEvent(
                    category="tts",
                    resource=tts_model,
                    quantity=len(speech_input),
                    unit="tts-character",
                    source="orchestrator",
                    source_id=uuid.uuid4().hex,
                    ts=_now(),
                    user_id=user_id,
                    project_id=project_id,
                    ref_kind="thread",
                    ref_id=ref_id,
                    details={"voice": voice},
                )
            ],
        )
    return speech_input, audio


# Fixed, self-descriptive preview phrases (one per supported UI language). Kept
# short — under _FORMULATION_MIN_LEN — so the preview never invokes the aux
# rewrite LLM, and identical every time so the audio cache serves repeat
# previews of the same voice instantly (the cache key is model+voice+text).
_PREVIEW_TEXT = {
    "en": "Hi — this is how I'll sound when I read your messages aloud.",
    "de": "Hallo — so klinge ich, wenn ich deine Nachrichten vorlese.",
}

# Upper bound on user-supplied preview text. Generous enough to audition a
# voice on a real sentence or two, short enough that a bake-off across voices
# stays cheap (each unique text is one cache entry + one synth). Enforced at
# the endpoint (422); the service clamps defensively for any other caller.
_PREVIEW_TEXT_MAX = 500


async def synthesize_voice_preview(
    *,
    voice: Optional[str],
    language: str,
    user_id: str,
    postgres_db,
    text: Optional[str] = None,
    ledger: Optional[UsageLedger] = None,
) -> Optional[bytes]:
    """Synthesize a short canned phrase in ``voice`` so the settings voice
    picker can preview how it sounds before the user commits to it.

    ``voice`` is the *candidate* voice — it may not be the saved default yet.
    An empty/``None`` value means "Auto", resolved exactly like real read-aloud
    via :func:`_pick_voice` (admin single voice → per-language map → built-in
    default). Uses the user's configured TTS model. No aux rewrite (the phrase
    is already clean and short), so it's cheap and low-latency, and the fixed
    phrase makes the audio cache near-100% effective on repeat previews.

    ``text`` overrides the canned phrase so the user can audition a voice on
    their own words (e.g. a name, a German sentence). It is spoken verbatim —
    no aux rewrite — and clamped to ``_PREVIEW_TEXT_MAX``. A distinct text is a
    distinct cache key, so a bake-off across voices with the same sentence
    still serves repeats from cache.

    Returns MP3 bytes, ``None`` when no TTS model is configured (endpoint maps
    to ``204``), or raises :class:`TtsSynthesisError` when a configured model
    fails to synthesize (endpoint maps to ``502``) — same contract as
    :func:`generate_message_tts`.
    """
    user_settings = await postgres_db.get_user_settings(user_id) or {}
    resolved_keys = await postgres_db.resolve_api_keys_for_job(
        user_id=user_id, project_id=None
    )
    tts_creds = await _resolve_capability_credentials(
        capability="tts",
        user_settings=user_settings,
        user_id=user_id,
        resolved_keys=resolved_keys,
        postgres_db=postgres_db,
    )
    if tts_creds is None:
        logger.info("No TTS model configured for user %s (voice preview)", user_id)
        return None
    tts_model = tts_creds.wire_model

    tts_params = tts_creds.params
    lang = "de" if (language or "").lower().startswith("de") else "en"
    spoken = (text or "").strip()[:_PREVIEW_TEXT_MAX] or _PREVIEW_TEXT[lang]
    candidate = (voice or "").strip() or None
    resolved_voice = _pick_voice(tts_params, lang, candidate)
    instructions = (tts_params.get("instructions") or "").strip() or None

    audio_key = _hash_key(tts_model, resolved_voice, instructions or "", spoken)
    async with _audio_lock:
        cached = _cache_get(_audio_cache, audio_key)
    if cached is not None:
        logger.debug("TTS preview cache hit (voice=%s)", resolved_voice)
        return cached

    audio = await _synthesize_speech(
        spoken,
        model=tts_model,
        voice=resolved_voice,
        base_url=tts_creds.base_url,
        api_key=tts_creds.api_key,
        instructions=instructions,
        provider=tts_params.get("provider"),
    )
    if not audio:
        raise TtsSynthesisError(f"TTS preview synthesis failed for model {tts_model}")
    async with _audio_lock:
        _cache_put(_audio_cache, audio_key, audio, _AUDIO_CACHE_MAX)
    if _metering_enabled(ledger):
        await _record_usage(
            ledger,
            [
                UsageEvent(
                    category="tts",
                    resource=tts_model,
                    quantity=len(spoken),
                    unit="tts-character",
                    source="orchestrator",
                    source_id=uuid.uuid4().hex,
                    ts=_now(),
                    user_id=user_id,
                    project_id=None,
                    ref_kind=None,
                    ref_id=None,
                    details={"voice": resolved_voice, "preview": True},
                )
            ],
        )
    return audio


# ---------------------------------------------------------------------------
# Chunk planning — split a long message into ordered, speakable chunks so the
# UI can synthesize + play them one after another (no truncation, no single
# multi-minute request). The LLM picks natural breakpoints; code guarantees
# the 4096-char ceiling and never fully fails.
# ---------------------------------------------------------------------------


def _strip_markdown_for_speech(text: str) -> str:
    """Local, dependency-free markdown → speakable-text cleanup for the fallback
    path (no aux LLM, or the aux failed). It doesn't *rewrite* prose the way the
    LLM does — it just stops the TTS engine from literally reading "asterisk
    asterisk", table pipes, and header hashes. Conservative on purpose: it leaves
    single underscores (snake_case identifiers) and ordinary punctuation alone.
    """
    if not text:
        return ""
    t = text
    # Fenced code → a short spoken placeholder (never read code or backticks aloud).
    t = re.sub(r"```[\s\S]*?```", " (code snippet) ", t)
    t = re.sub(r"~~~[\s\S]*?~~~", " (code snippet) ", t)
    # Images ![alt](url) → drop; links [text](url) → text; inline `code` → code.
    # A destination may contain one bracketed IPv6 authority. Other openers
    # terminate an attempt, so malformed repeated links cannot rescan a tail.
    destination = (
        r"(?:(?:https?:)?//(?:[^/@\s\[\]()]+@)?\[[^\[\]\n]+\][^)\[\n]*"
        r"|[^)\[\n]*)"
    )
    t = re.sub(r"!\[[^\]\[\n]*\]\(" + destination + r"\)", " ", t, flags=re.I)
    t = re.sub(r"\[([^\]\[\n]*)\]\(" + destination + r"\)", r"\1", t, flags=re.I)
    t = re.sub(r"`([^`]*)`", r"\1", t)

    # Line-oriented cleanup: tables → "cell, cell." sentences, and strip leading
    # header / quote / list markers.
    out_lines: list[str] = []
    for line in t.split("\n"):
        s = line.strip()
        is_table_row = s.startswith("|") or (
            s.count("|") >= 2 and bool(re.search(r"\S\s*\|\s*\S", s))
        )
        if is_table_row:
            # Drop the |---|:--| separator rows entirely.
            if re.fullmatch(r"\|?[\s:|-]+\|?", s):
                continue
            cells = [c.strip() for c in s.strip().strip("|").split("|")]
            cells = [c for c in cells if c]
            if cells:
                out_lines.append(", ".join(cells) + ".")
            continue
        line = re.sub(r"^\s{0,3}#{1,6}\s*", "", line)  # headers
        line = re.sub(r"^\s{0,3}>\s?", "", line)  # blockquotes
        line = re.sub(r"^\s*[-*+]\s+", "", line)  # bullets
        line = re.sub(r"^\s*\d+[.)]\s+", "", line)  # ordered list
        stripped_line = line.strip()
        if (  # horizontal rule
            re.fullmatch(r"-{3,}", stripped_line)
            or re.fullmatch(r"\*{3,}", stripped_line)
            or re.fullmatch(r"_{3,}", stripped_line)
        ):
            continue
        out_lines.append(line)
    t = "\n".join(out_lines)

    # Emphasis markers (the "asterisk asterisk" complaint) and stray pipes.
    t = re.sub(r"\*+", "", t)
    t = t.replace("~~", "").replace("__", "").replace("|", " ")
    # Tidy whitespace.
    t = re.sub(r"[ \t]{2,}", " ", t)
    t = re.sub(r"\n{3,}", "\n\n", t)
    return t.strip()


def _clean_stream_chunk(text: str) -> str:
    """Trim one streamed chunk, defensively removing any stray sentinel or code
    fence the model tucked in (drain already split on full sentinels)."""
    if not text:
        return ""
    t = text.replace(TTS_CHUNK_SENTINEL, " ").strip()
    if t.startswith("```"):
        t = re.sub(r"^```[a-zA-Z]*\n?", "", t)
        t = re.sub(r"\n?```$", "", t).strip()
    return t


def _drain_sentinels(buffer: str) -> tuple[list[str], str]:
    """Split an accumulating stream ``buffer`` on the chunk sentinel. Returns
    ``(complete_chunks, remainder)`` — the pieces before each sentinel (trimmed,
    empties dropped) and the still-accumulating tail after the last sentinel. A
    partial sentinel split across deltas stays in the remainder until complete."""
    if TTS_CHUNK_SENTINEL not in buffer:
        return [], buffer
    parts = buffer.split(TTS_CHUNK_SENTINEL)
    remainder = parts.pop()
    chunks = [p.strip() for p in parts]
    return [c for c in chunks if c], remainder


def _split_text_into_chunks(text: str, limit: int = TTS_CHUNK_LIMIT) -> list[str]:
    """Deterministically pack ``text`` into ``<=limit``-char chunks, breaking at
    paragraph then sentence boundaries. The fallback when the LLM can't chunk
    (unavailable, errored, truncated) and the hard backstop behind the gate.
    Always terminates; every returned chunk is ``<= limit``.
    """
    text = (text or "").strip()
    if not text:
        return []
    if len(text) <= limit:
        return [text]

    # Break into atomic segments small enough to pack: paragraphs, then
    # sentences, then (last resort) hard slices of an over-long sentence.
    segments: list[str] = []
    for para in re.split(r"\n\s*\n", text):
        para = para.strip()
        if not para:
            continue
        if len(para) <= limit:
            segments.append(para)
            continue
        for sentence in re.split(r"(?<=[.!?])\s+", para):
            sentence = sentence.strip()
            if not sentence:
                continue
            if len(sentence) <= limit:
                segments.append(sentence)
            else:
                for i in range(0, len(sentence), limit):
                    segments.append(sentence[i : i + limit])

    chunks: list[str] = []
    current = ""
    for segment in segments:
        if not current:
            current = segment
        elif len(current) + 2 + len(segment) <= limit:
            current = f"{current}\n\n{segment}"
        else:
            chunks.append(current)
            current = segment
    if current:
        chunks.append(current)
    return chunks


def _split_once_under(text: str, target: int) -> tuple[str, str]:
    """Split ``text`` once at the latest natural boundary at/under ``target``
    chars — a sentence end preferred, then a paragraph break. Returns
    ``(head, tail)``; ``tail`` is empty when no clean boundary exists under
    ``target`` (never breaks mid-sentence, so a single over-long opening
    sentence is left whole)."""
    if len(text) <= target:
        return text, ""
    window = text[:target]
    sentence_ends = list(re.finditer(r"[.!?](?:\s|$)", window))
    if sentence_ends:
        cut = sentence_ends[-1].end()
        head, tail = text[:cut].strip(), text[cut:].strip()
        if head and tail:
            return head, tail
    nl = window.rfind("\n")
    if nl > 0:
        head, tail = text[:nl].strip(), text[nl:].strip()
        if head and tail:
            return head, tail
    return text, ""


def _shorten_first_chunk(
    chunks: list[str], target: int = TTS_FIRST_CHUNK_TARGET
) -> list[str]:
    """Keep the FIRST chunk small (~``target`` chars) so chunk 1 synthesizes fast
    and playback starts sooner while later chunks generate. Splits only at a
    natural boundary; a first chunk already under ``target`` (or an unsplittable
    long opening sentence) is left untouched."""
    if not chunks:
        return chunks
    head, tail = _split_once_under(chunks[0], target)
    if tail:
        return [head, tail, *chunks[1:]]
    return chunks


def _enforce_chunk_limit(chunks: list[str], limit: int = TTS_CHUNK_LIMIT) -> list[str]:
    """Hard guarantee that every chunk is ``<= limit``. Any oversized chunk is
    deterministically re-split — the last resort after the LLM had its chance."""
    result: list[str] = []
    for chunk in chunks:
        chunk = (chunk or "").strip()
        if not chunk:
            continue
        if len(chunk) <= limit:
            result.append(chunk)
        else:
            result.extend(_split_text_into_chunks(chunk, limit))
    return result


def _parse_chunk_array(raw: str) -> Optional[list[str]]:
    """Parse the LLM's chunk output into a list of non-empty strings, tolerating
    code fences and surrounding prose. ``None`` when no usable array is found."""
    if not raw:
        return None
    text = raw.strip()
    if text.startswith("```"):
        text = re.sub(r"^```[a-zA-Z]*\n?", "", text)
        text = re.sub(r"\n?```$", "", text).strip()
    start, end = text.find("["), text.rfind("]")
    if start != -1 and end > start:
        text = text[start : end + 1]
    try:
        parsed = json.loads(text)
    except (ValueError, TypeError):
        return None
    if not isinstance(parsed, list):
        return None
    chunks = [
        str(item).strip()
        for item in parsed
        if isinstance(item, (str, int, float)) and str(item).strip()
    ]
    return chunks or None


async def _resplit_oversized(
    chunks: list[str],
    *,
    model: str,
    base_url: Optional[str],
    api_key: Optional[str],
    timeout: float = 60.0,
) -> list[str]:
    """Re-prompt the LLM to split any chunk that exceeds the limit at a natural
    break (one pass, in order). Anything still over is left for the code gate."""
    client = AsyncOpenAI(
        api_key=api_key, base_url=base_url, timeout=timeout, max_retries=0
    )
    result: list[str] = []
    prompt = (
        f"Split the following text into a JSON array of chunks, each under "
        f"{TTS_CHUNK_TARGET} characters, breaking ONLY at natural stopping "
        f"points (end of a sentence or paragraph). Return ONLY the JSON array."
    )
    try:
        for chunk in chunks:
            if len(chunk) <= TTS_CHUNK_LIMIT:
                result.append(chunk)
                continue
            try:
                response = await client.chat.completions.create(
                    model=model,
                    messages=[
                        {"role": "system", "content": prompt},
                        {"role": "user", "content": chunk},
                    ],
                    temperature=0.3,
                    extra_body=_aux_extra_body(model),
                )
                sub = _parse_chunk_array(response.choices[0].message.content or "")
                result.extend(sub if sub else [chunk])
            except Exception:
                logger.exception("TTS re-split call failed; deferring to code gate")
                result.append(chunk)
    finally:
        await client.close()
    return result


async def _llm_clean_and_chunk(
    text: str,
    *,
    model: str,
    base_url: Optional[str],
    api_key: Optional[str],
    timeout: float = 30.0,
    ledger: Optional[UsageLedger] = None,
    user_id: Optional[str] = None,
    ref_id: Optional[str] = None,
    custom_prompt: Optional[str] = None,
    reasoning_level: Optional[str] = None,
) -> Optional[list[str]]:
    """LLM cleanup + natural chunking → ordered chunk list, or ``None`` to signal
    the caller should fall back to deterministic splitting.

    ``None`` on: no key, call error, truncated output (hit max tokens — the
    array would be incomplete), or unparseable output. Oversized chunks are
    re-prompted once; the caller's gate enforces the hard ceiling regardless.
    ``custom_prompt`` / ``reasoning_level`` come from the user's ``read_aloud``
    settings (see :func:`_augment_rewrite_prompt` / :func:`_aux_reasoning_body`).
    """
    if not api_key:
        return None
    client = AsyncOpenAI(
        api_key=api_key, base_url=base_url, timeout=timeout, max_retries=0
    )
    try:
        response = await client.chat.completions.create(
            model=model,
            messages=[
                {
                    "role": "system",
                    "content": _augment_rewrite_prompt(
                        CHUNKING_SYSTEM_PROMPT, custom_prompt
                    ),
                },
                {"role": "user", "content": text},
            ],
            temperature=0.3,
            extra_body=_aux_extra_body(model, reasoning_level),
        )
        if _metering_enabled(ledger):
            # Meter tokens even when the plan is truncated/unparseable below —
            # the tokens were still consumed.
            await _record_usage(
                ledger,
                _llm_token_events(
                    response,
                    model=model,
                    stage="chunk-plan",
                    user_id=user_id,
                    ref_id=ref_id,
                ),
            )
        choice = response.choices[0]
        if getattr(choice, "finish_reason", None) == "length":
            logger.warning(
                "TTS chunk plan truncated (max tokens; stage=chunk-plan model=%s "
                "base_url=%s); using deterministic split",
                model,
                base_url,
            )
            return None
        # Strip any leaked <think> before parsing — a reasoning preamble would
        # otherwise break the JSON-array parse (→ fallback) or ride into a chunk.
        chunks = _parse_chunk_array(_strip_think_tags(choice.message.content or ""))
    except Exception:
        logger.exception(
            "TTS chunk-planning LLM call failed (stage=chunk-plan model=%s "
            "base_url=%s); using deterministic split",
            model,
            base_url,
        )
        return None
    finally:
        await client.close()

    if not chunks:
        return None
    if any(len(chunk) > TTS_CHUNK_LIMIT for chunk in chunks):
        chunks = await _resplit_oversized(
            chunks, model=model, base_url=base_url, api_key=api_key
        )
    return chunks


async def plan_tts_chunks(
    *,
    content: str,
    user_id: str,
    postgres_db,
    ledger: Optional[UsageLedger] = None,
    ref_id: Optional[str] = None,
    project_id: Optional[str] = None,
    reformulate: bool = True,
) -> Optional[dict]:
    """Clean ``content`` for speech and split it into ordered ``<=4096``-char
    chunks at natural breakpoints, for sequential synthesis + playback.

    Returns ``{"chunks": [...], "rewritten": bool}`` — ``rewritten`` is ``True``
    only when the auxiliary LLM actually cleaned the text; ``False`` when the
    deterministic splitter ran the (markdown-stripped) raw text — no aux model,
    the LLM failed, or ``reformulate=False`` (the client's "read it as-is"
    bailout) — so the UI can honestly say "rewriting skipped". Even in that
    fallback the markdown is stripped so the TTS engine never reads "asterisk
    asterisk" or table pipes aloud. The first chunk is kept short for fast
    time-to-first-audio. Returns ``None`` when no TTS model is configured (the
    endpoint maps this to ``204``); otherwise ``chunks`` always has ≥1 entry.

    This is the buffered planner (one round trip, all chunks at once). See
    :func:`stream_tts_chunks` for the streaming variant the UI prefers.
    """
    if not content or not content.strip():
        return None

    # Load user_settings first so the rewrite variant (custom prompt + reasoning
    # level) is part of the cache key — otherwise editing the read-aloud prompt
    # would replay a stale rewrite. Only the reformulate path uses it; the raw
    # bailout keeps a constant key so its cache hit-rate is unaffected.
    user_settings = await postgres_db.get_user_settings(user_id) or {}
    custom_prompt, reasoning_level = _read_aloud_prefs(user_settings)
    variant = (
        _rewrite_variant_key(custom_prompt, reasoning_level) if reformulate else ""
    )
    cache_key = _hash_key("plan", "reform" if reformulate else "raw", variant, content)
    async with _plan_lock:
        cached = _cache_get(_plan_cache, cache_key)
    if cached is not None:
        logger.debug("TTS chunk-plan cache hit")
        return {"chunks": list(cached["chunks"]), "rewritten": cached["rewritten"]}

    resolved_keys = await postgres_db.resolve_api_keys_for_job(
        user_id=user_id, project_id=None
    )

    # A TTS model must be configured at all, so the endpoint can 204 cleanly
    # (the chunks are useless without something to synthesize them).
    tts_creds = await _resolve_capability_credentials(
        capability="tts",
        user_settings=user_settings,
        user_id=user_id,
        resolved_keys=resolved_keys,
        postgres_db=postgres_db,
    )
    if tts_creds is None:
        logger.info("No TTS model configured for user %s; cannot plan chunks", user_id)
        return None

    chunks: Optional[list[str]] = None
    rewritten = False
    if reformulate:
        aux_creds = await _resolve_capability_credentials(
            capability="auxiliary",
            user_settings=user_settings,
            user_id=user_id,
            resolved_keys=resolved_keys,
            postgres_db=postgres_db,
        )
        if aux_creds is not None:
            chunks = await _llm_clean_and_chunk(
                content,
                model=aux_creds.wire_model,
                base_url=aux_creds.base_url,
                api_key=aux_creds.api_key,
                ledger=ledger,
                user_id=user_id,
                ref_id=ref_id,
                custom_prompt=custom_prompt,
                reasoning_level=reasoning_level,
            )
            # rewritten iff the LLM actually produced the chunks (not fallback).
            rewritten = chunks is not None

    # Fallback: deterministic split of the *markdown-stripped* content — no LLM
    # cleanup, but always playable and free of "asterisk asterisk". Runs when
    # there's no aux model, the LLM couldn't deliver, or reformulate=False (the
    # UI bailout). Pack to the synthesis-sized target, not the 4096 ceiling, so
    # each chunk stays fast to synthesize.
    if not chunks:
        chunks = _split_text_into_chunks(
            _strip_markdown_for_speech(content), TTS_CHUNK_TARGET
        )

    # Short first chunk → fast time-to-first-audio (later chunks generate while
    # chunk 1 plays). Applied to both the LLM and deterministic paths.
    chunks = _shorten_first_chunk(chunks)

    # Hard gate: nothing over the ceiling reaches synthesis.
    chunks = _enforce_chunk_limit(chunks)
    if not chunks:
        chunks = _split_text_into_chunks(
            _strip_markdown_for_speech(content), TTS_CHUNK_TARGET
        ) or [content.strip()]

    result = {"chunks": chunks, "rewritten": rewritten}
    async with _plan_lock:
        _cache_put(_plan_cache, cache_key, result, _PLAN_CACHE_MAX)
    return {"chunks": list(chunks), "rewritten": rewritten}


def _emit_pieces(text: str) -> list[str]:
    """Clean one about-to-be-yielded chunk (strip stray sentinel/fences) and
    hard-gate to the 4096 ceiling. Sizing is done by the streaming flush loop
    (which cuts at the target), so this only guards the ceiling as a backstop."""
    text = _clean_stream_chunk(text)
    if not text:
        return []
    return _enforce_chunk_limit([text])


async def stream_tts_chunks(
    *,
    content: str,
    user_id: str,
    postgres_db,
    ledger: Optional[UsageLedger] = None,
    ref_id: Optional[str] = None,
    project_id: Optional[str] = None,
):
    """Streaming counterpart of :func:`plan_tts_chunks`: an async generator that
    yields each speakable chunk the *moment* it's ready, so the client can
    synthesize + start playing chunk 1 while the rest still generate. This is
    what retires the timeout failure mode — there is no single long call to time
    out, only a trickle bounded by a per-token idle timeout (not a total cap), so
    a long rewrite of complex content is fine.

    Yields event dicts:
      ``{"type": "unavailable"}``                         — no TTS model configured
      ``{"type": "chunk", "index", "text", "rewritten"}`` — one ready chunk
      ``{"type": "done", "total", "rewritten"}``          — terminal

    ``rewritten`` is ``True`` while the aux LLM is producing the chunks, ``False``
    on the deterministic (markdown-stripped) fallback used when there's no aux
    model or the stream fails/stalls before producing anything usable. A
    mid-stream failure keeps the chunks already delivered (partial read) and ends
    with ``done``.
    """
    if not content or not content.strip():
        yield {"type": "done", "total": 0, "rewritten": False}
        return

    user_settings = await postgres_db.get_user_settings(user_id) or {}
    resolved_keys = await postgres_db.resolve_api_keys_for_job(
        user_id=user_id, project_id=None
    )

    tts_creds = await _resolve_capability_credentials(
        capability="tts",
        user_settings=user_settings,
        user_id=user_id,
        resolved_keys=resolved_keys,
        postgres_db=postgres_db,
    )
    if tts_creds is None:
        logger.info(
            "No TTS model configured for user %s; cannot stream chunks", user_id
        )
        yield {"type": "unavailable"}
        return

    aux_creds = await _resolve_capability_credentials(
        capability="auxiliary",
        user_settings=user_settings,
        user_id=user_id,
        resolved_keys=resolved_keys,
        postgres_db=postgres_db,
    )

    def _fallback_chunks() -> list[str]:
        chunks = _split_text_into_chunks(
            _strip_markdown_for_speech(content), TTS_CHUNK_TARGET
        )
        chunks = _enforce_chunk_limit(_shorten_first_chunk(chunks))
        return chunks or [_strip_markdown_for_speech(content) or content.strip()]

    index = 0

    # No aux model (or no key) → deterministic markdown-stripped split, streamed
    # out as-is. Still incremental for the client, just not LLM-rewritten.
    aux_key = aux_creds.api_key if aux_creds else None
    if not aux_creds or not aux_key:
        for c in _fallback_chunks():
            yield {"type": "chunk", "index": index, "text": c, "rewritten": False}
            index += 1
        yield {"type": "done", "total": index, "rewritten": False}
        return

    aux_model = aux_creds.wire_model
    aux_base_url = aux_creds.base_url
    client = AsyncOpenAI(
        api_key=aux_creds.api_key,
        base_url=aux_base_url,
        timeout=_STREAM_MAX_TOTAL,
        max_retries=0,
    )
    buffer = ""
    produced_any = False
    got_content = False
    usage_obj: Any = None
    stalled = False
    # Streaming <think>-strip state: `think_raw` accumulates the RAW model output;
    # `think_emitted` is how much of the think-stripped text we've already fed to
    # `buffer`. Only new clean (post-reasoning) content reaches the chunker.
    think_raw = ""
    think_emitted = 0
    try:
        custom_prompt, reasoning_level = _read_aloud_prefs(user_settings)
        stream = await client.chat.completions.create(
            model=aux_model,
            messages=[
                {
                    "role": "system",
                    "content": _augment_rewrite_prompt(
                        CHUNKING_STREAM_SYSTEM_PROMPT, custom_prompt
                    ),
                },
                {"role": "user", "content": content},
            ],
            temperature=0.3,
            stream=True,
            stream_options={"include_usage": True},
            extra_body=_aux_extra_body(aux_model, reasoning_level),
        )
        stream_iter = stream.__aiter__()
        while True:
            # Patient before the first content token, tight once flowing.
            gap_timeout = (
                _STREAM_IDLE_TIMEOUT if got_content else _STREAM_FIRST_TOKEN_TIMEOUT
            )
            try:
                event = await asyncio.wait_for(
                    stream_iter.__anext__(), timeout=gap_timeout
                )
            except StopAsyncIteration:
                break
            except asyncio.TimeoutError:
                logger.warning(
                    "TTS chunk stream timeout after %.0fs (stage=chunk-stream "
                    "model=%s base_url=%s got_content=%s produced=%s)",
                    gap_timeout,
                    aux_model,
                    aux_base_url,
                    got_content,
                    produced_any,
                )
                stalled = True
                break

            u = getattr(event, "usage", None)
            if u is not None:
                usage_obj = u
            choices = getattr(event, "choices", None)
            if not choices:
                continue
            delta = getattr(choices[0], "delta", None)
            piece = getattr(delta, "content", None) if delta is not None else None
            if not piece:
                continue
            got_content = True
            # Route the raw delta through the streaming think-strip: only the
            # newly-safe clean text (reasoning removed / withheld) hits the buffer.
            think_raw += piece
            clean = _strip_think_stream(think_raw)
            if len(clean) <= think_emitted:
                continue  # still inside reasoning, or only a partial tag arrived
            buffer += clean[think_emitted:]
            think_emitted = len(clean)

            # (1) Honour an explicit sentinel boundary if the model emitted one
            #     (bonus — most local models don't; see the module note).
            ready, buffer = _drain_sentinels(buffer)
            for chunk_text in ready:
                for pe in _emit_pieces(chunk_text):
                    produced_any = True
                    yield {
                        "type": "chunk",
                        "index": index,
                        "text": pe,
                        "rewritten": True,
                    }
                    index += 1

            # (2) Size-based flush — the real incremental mechanism. Whenever the
            #     buffer crosses the target, cut at the latest natural sentence/
            #     paragraph boundary under it and emit. The first chunk uses the
            #     smaller first-chunk target for fast time-to-first-audio.
            while len(buffer) >= (
                TTS_FIRST_CHUNK_TARGET if index == 0 else TTS_CHUNK_TARGET
            ):
                target = TTS_FIRST_CHUNK_TARGET if index == 0 else TTS_CHUNK_TARGET
                head, tail = _split_once_under(buffer, target)
                if tail:
                    buffer = tail
                elif len(buffer) >= TTS_CHUNK_LIMIT:
                    # A run this long with no boundary — hard-cut at the ceiling.
                    head, buffer = buffer[:TTS_CHUNK_LIMIT], buffer[TTS_CHUNK_LIMIT:]
                else:
                    break  # no boundary yet; wait for more tokens
                for pe in _emit_pieces(head):
                    produced_any = True
                    yield {
                        "type": "chunk",
                        "index": index,
                        "text": pe,
                        "rewritten": True,
                    }
                    index += 1

        # Final think-strip flush: release any tail we were withholding as a
        # possible partial <think> tag (the stream ended → it's real content).
        final_clean = _strip_think_stream(think_raw, final=True)
        if len(final_clean) > think_emitted:
            buffer += final_clean[think_emitted:]
            think_emitted = len(final_clean)

        # Flush the trailing remainder (the final chunk), unless we bailed on an
        # idle stall mid-message (the buffer is partial then).
        if not stalled:
            for pe in _emit_pieces(buffer):
                produced_any = True
                yield {"type": "chunk", "index": index, "text": pe, "rewritten": True}
                index += 1
    except Exception:
        logger.exception(
            "TTS chunk-stream failed (stage=chunk-stream model=%s base_url=%s "
            "produced=%s); falling back to deterministic split",
            aux_model,
            aux_base_url,
            produced_any,
        )
    finally:
        if usage_obj is not None and _metering_enabled(ledger):
            await _record_usage(
                ledger,
                _token_events_from_usage(
                    usage_obj,
                    model=aux_model,
                    stage="chunk-stream",
                    user_id=user_id,
                    ref_id=ref_id,
                ),
            )
        with contextlib.suppress(Exception):
            await client.close()

    if produced_any:
        yield {"type": "done", "total": index, "rewritten": True}
        return

    # The stream produced nothing usable (never connected, errored early, stalled
    # before a chunk, or returned only whitespace) → deterministic fallback so the
    # button never dead-ends.
    for c in _fallback_chunks():
        yield {"type": "chunk", "index": index, "text": c, "rewritten": False}
        index += 1
    yield {"type": "done", "total": index, "rewritten": False}
