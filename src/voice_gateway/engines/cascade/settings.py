"""Settings of a ``cascade`` profile; knowledge-base/knowledge/features/local_voice_gateway.md §4.3.

A profile names three OpenAI-compatible stages (STT, voice LLM, TTS), each a
``base_url`` and ``model`` with an optional key file, plus turn, barge-in and
filler settings that default to the note's values. Parsing reads key files
once, at config load, and never puts a key or a path's content in a message.
This module imports nothing heavy, so a config check runs without Pipecat.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlsplit

from voice_gateway.errors import ConfigError

#: Short German backchannels a caller says while the voice talks. While the
#: voice speaks, a stretch that is only these is not a turn.
GERMAN_BACKCHANNELS = (
    "ja",
    "jaja",
    "ja ja",
    "mhm",
    "mh",
    "hm",
    "hmm",
    "aha",
    "achso",
    "ach so",
    "okay",
    "ok",
    "genau",
    "gut",
    "richtig",
    "stimmt",
    "klar",
    "verstehe",
    "alles klar",
    "ja genau",
    "ah ja",
    "ah okay",
)
GERMAN_FILLER = "Einen Moment, ich schaue nach."
LANGUAGE_NAMES = {"de": "German", "en": "English"}


@dataclass(frozen=True)
class Stage:
    """One OpenAI-compatible endpoint. ``api_key`` is read from its file."""

    base_url: str
    model: str
    api_key: str | None = field(default=None, repr=False)
    extra_body: dict = field(default_factory=dict)


@dataclass(frozen=True)
class SttSettings(Stage):
    #: Whisper's initial prompt: vocabulary the transcription should prefer.
    prompt: str | None = None


@dataclass(frozen=True)
class LlmSettings(Stage):
    temperature: float = 0.3
    max_tokens: int = 300
    #: Role for the gateway's own notes (back-office commentary, thinking,
    #: steering instructions): "system" when the model's chat template accepts
    #: a system message mid-conversation, else "user".
    note_role: str = "system"
    #: Prefill the fixed prompt prefix (rules, instructions, tool) while the
    #: greeting plays, so the caller's first turn hits a warm cache.
    prewarm: bool = True


@dataclass(frozen=True)
class TtsSettings(Stage):
    voice: str = ""
    #: GPT-Live voice names (the expert's ``live.voice``) mapped to this
    #: server's voices; an unmapped name gets ``voice``.
    voices: dict = field(default_factory=dict)
    #: "pcm" (raw 16-bit mono at ``sample_rate``) or "wav" (rate in header).
    response_format: str = "pcm"
    sample_rate: int = 24_000
    speed: float | None = None
    #: A reply's first sentence may start at a clause (comma, "und", …) once
    #: this many characters are written; 0 waits for whole sentences.
    clause_chars: int = 30
    #: Keep the audio of short recurring texts (filler, greeting, holding
    #: phrases) in memory and replay it; for a TTS that is slow to start.
    cache_phrases: bool = True


@dataclass(frozen=True)
class TurnSettings:
    vad_confidence: float = 0.7
    vad_start_secs: float = 0.2
    vad_stop_secs: float = 0.2
    vad_min_volume: float = 0.6
    #: A Silero ONNX file to use instead of the one bundled with Pipecat.
    vad_model_file: str | None = None
    #: Smart Turn v3 decides the end of a turn; without it, a fixed silence.
    smart_turn: bool = True
    #: Longest silence Smart Turn waits for when it reads a turn as unfinished.
    smart_turn_stop_secs: float = 3.0
    #: The silence that ends a turn when ``smart_turn`` is off.
    speech_timeout_secs: float = 0.6


@dataclass(frozen=True)
class BargeInSettings:
    #: Caller speech the voice talks over before it stops for the caller.
    min_speech_secs: float = 0.5
    #: Words that let a short stretch interrupt anyway (counted on its
    #: transcript once the stretch ended).
    min_words: int = 2
    backchannels: tuple[str, ...] = GERMAN_BACKCHANNELS


@dataclass(frozen=True)
class CascadeSettings:
    name: str
    stt: SttSettings
    llm: LlmSettings
    tts: TtsSettings
    language: str = "de"
    filler: str = GERMAN_FILLER
    turn: TurnSettings = TurnSettings()
    barge_in: BargeInSettings = BargeInSettings()
    #: How far the engine may queue audio ahead of what the caller heard.
    #: Small keeps barge-in and the "what was heard" bookkeeping exact.
    playout_lead_ms: int = 200
    #: Extra lines appended to the engine's prompt (deployment wording).
    prompt: str = ""


def _mapping(raw, label: str, allowed: set[str]) -> dict:
    if not isinstance(raw, dict):
        raise ConfigError(f"{label} must be a mapping")
    unknown = sorted(str(k) for k in raw.keys() - allowed)
    if unknown:
        raise ConfigError(f"{label} has unknown fields: {', '.join(unknown)}")
    return raw


def _text(raw: dict, key: str, label: str, default: str | None = None) -> str:
    value = raw.get(key, default)
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"{label}.{key} must be a nonempty string")
    return value.strip()


def _optional_text(raw: dict, key: str, label: str) -> str | None:
    value = raw.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise ConfigError(f"{label}.{key} must be a string")
    return value.strip() or None


def _number(
    raw: dict, key: str, label: str, default: float, low: float, high: float
) -> float:
    value = raw.get(key, default)
    if (
        isinstance(value, bool)
        or not isinstance(value, int | float)
        or not math.isfinite(value)
        or not low <= value <= high
    ):
        raise ConfigError(f"{label}.{key} must be a number from {low} to {high}")
    return float(value)


def _integer(raw: dict, key: str, label: str, default: int, low: int, high: int) -> int:
    value = raw.get(key, default)
    if type(value) is not int or not low <= value <= high:
        raise ConfigError(f"{label}.{key} must be an integer from {low} to {high}")
    return value


def _boolean(raw: dict, key: str, label: str, default: bool) -> bool:
    value = raw.get(key, default)
    if not isinstance(value, bool):
        raise ConfigError(f"{label}.{key} must be true or false")
    return value


def _base_url(raw: dict, label: str) -> str:
    url = _text(raw, "base_url", label).rstrip("/")
    try:
        parts = urlsplit(url)
        if (
            parts.scheme not in {"http", "https"}
            or not parts.hostname
            or parts.username
            or parts.password
            or parts.query
            or parts.fragment
        ):
            raise ValueError
        _ = parts.port
    except ValueError:
        raise ConfigError(
            f"{label}.base_url must be an http(s) URL without credentials, query or fragment"
        ) from None
    return url


def _file(value: str, base: Path | None, label: str) -> Path:
    """A settings file, relative to the config file's directory; without a
    config file (the scripts read a profile directly) it must be absolute."""
    path = Path(value)
    if path.is_absolute():
        return path
    if base is None:
        raise ConfigError(f"{label} must be an absolute path")
    return base / path


def _key(raw: dict, label: str, base: Path | None) -> str | None:
    value = raw.get("api_key_file")
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"{label}.api_key_file must be a nonempty string")
    path = _file(value.strip(), base, f"{label}.api_key_file")
    try:
        key = path.read_text().strip()
    except (OSError, UnicodeError, ValueError):
        raise ConfigError(f"cannot read {label}.api_key_file") from None
    if not key or any(c <= " " or c >= "\x7f" for c in key):
        raise ConfigError(
            f"{label}.api_key_file must hold one printable ASCII key without whitespace"
        )
    return key


def _extra_body(raw: dict, label: str) -> dict:
    value = raw.get("extra_body", {})
    if not isinstance(value, dict) or not all(isinstance(k, str) for k in value):
        raise ConfigError(f"{label}.extra_body must be a mapping")
    return dict(value)


_STAGE_KEYS = {"base_url", "model", "api_key_file", "extra_body"}


def _stage(raw, label: str, extra: set[str], base: Path | None) -> dict:
    raw = _mapping(raw, label, _STAGE_KEYS | extra)
    return {
        "base_url": _base_url(raw, label),
        "model": _text(raw, "model", label),
        "api_key": _key(raw, label, base),
        "extra_body": _extra_body(raw, label),
    }


def _stt(raw, label: str, base: Path | None) -> SttSettings:
    stage = _stage(raw, label, {"prompt"}, base)
    return SttSettings(**stage, prompt=_optional_text(raw, "prompt", label))


def _llm(raw, label: str, base: Path | None) -> LlmSettings:
    stage = _stage(
        raw, label, {"temperature", "max_tokens", "note_role", "prewarm"}, base
    )
    role = raw.get("note_role", "system")
    if role not in {"system", "user", "developer"}:
        raise ConfigError(f"{label}.note_role must be system, user or developer")
    return LlmSettings(
        **stage,
        temperature=_number(raw, "temperature", label, 0.3, 0.0, 2.0),
        max_tokens=_integer(raw, "max_tokens", label, 300, 16, 4096),
        note_role=role,
        prewarm=_boolean(raw, "prewarm", label, True),
    )


def _tts(raw, label: str, base: Path | None) -> TtsSettings:
    stage = _stage(
        raw,
        label,
        {
            "voice",
            "voices",
            "response_format",
            "sample_rate",
            "speed",
            "clause_chars",
            "cache_phrases",
        },
        base,
    )
    voice = raw.get("voice", "")
    if not isinstance(voice, str):
        raise ConfigError(f"{label}.voice must be a string")
    voices = raw.get("voices", {})
    if not isinstance(voices, dict) or not all(
        isinstance(k, str) and isinstance(v, str) and v.strip()
        for k, v in voices.items()
    ):
        raise ConfigError(f"{label}.voices must map voice names to voice names")
    response_format = raw.get("response_format", "pcm")
    if response_format not in {"pcm", "wav"}:
        raise ConfigError(f"{label}.response_format must be pcm or wav")
    speed = raw.get("speed")
    return TtsSettings(
        **stage,
        voice=voice.strip(),
        voices={k: v.strip() for k, v in voices.items()},
        response_format=response_format,
        sample_rate=_integer(raw, "sample_rate", label, 24_000, 8_000, 48_000),
        speed=None if speed is None else _number(raw, "speed", label, 1.0, 0.5, 2.0),
        clause_chars=_integer(raw, "clause_chars", label, 30, 0, 500),
        cache_phrases=_boolean(raw, "cache_phrases", label, True),
    )


def _turn(raw, label: str, base: Path | None) -> TurnSettings:
    raw = _mapping(raw, label, set(TurnSettings.__dataclass_fields__))
    model_file = _optional_text(raw, "vad_model_file", label)
    if model_file is not None:
        path = _file(model_file, base, f"{label}.vad_model_file")
        if not path.is_file():
            raise ConfigError(f"{label}.vad_model_file must be an existing file")
        model_file = str(path)
    return TurnSettings(
        vad_confidence=_number(raw, "vad_confidence", label, 0.7, 0.05, 0.99),
        vad_start_secs=_number(raw, "vad_start_secs", label, 0.2, 0.05, 1.0),
        vad_stop_secs=_number(raw, "vad_stop_secs", label, 0.2, 0.05, 2.0),
        vad_min_volume=_number(raw, "vad_min_volume", label, 0.6, 0.0, 1.0),
        vad_model_file=model_file,
        smart_turn=_boolean(raw, "smart_turn", label, True),
        smart_turn_stop_secs=_number(
            raw, "smart_turn_stop_secs", label, 3.0, 0.3, 10.0
        ),
        speech_timeout_secs=_number(raw, "speech_timeout_secs", label, 0.6, 0.1, 5.0),
    )


def _barge_in(raw, label: str, language: str) -> BargeInSettings:
    raw = _mapping(raw, label, {"min_speech_secs", "min_words", "backchannels"})
    words = raw.get("backchannels")
    if words is None:
        words = GERMAN_BACKCHANNELS if language == "de" else ()
    elif not isinstance(words, list) or not all(
        isinstance(w, str) and w.strip() for w in words
    ):
        raise ConfigError(f"{label}.backchannels must be a list of words")
    return BargeInSettings(
        min_speech_secs=_number(raw, "min_speech_secs", label, 0.5, 0.0, 3.0),
        min_words=_integer(raw, "min_words", label, 2, 1, 10),
        backchannels=tuple(w.strip().lower() for w in words),
    )


_PROFILE_KEYS = {
    "stt",
    "llm",
    "tts",
    "language",
    "filler",
    "turn",
    "barge_in",
    "playout_lead_ms",
    "prompt",
}


def parse_settings(name: str, raw: dict, base: Path | None = None) -> CascadeSettings:
    """Validate one profile's settings (without ``engine``); ConfigError if unusable.

    Relative files resolve against ``base``, the config file's directory."""
    label = f"profile {name}"
    raw = _mapping(raw, label, _PROFILE_KEYS)
    for stage in ("stt", "llm", "tts"):
        if stage not in raw:
            raise ConfigError(f"{label} needs {stage}")
    language = _text(raw, "language", label, "de").lower()
    if language not in LANGUAGE_NAMES:
        raise ConfigError(
            f"{label}.language must be one of {', '.join(sorted(LANGUAGE_NAMES))}"
        )
    filler = raw.get("filler", GERMAN_FILLER if language == "de" else "One moment.")
    if not isinstance(filler, str):
        raise ConfigError(f"{label}.filler must be a string (empty for none)")
    prompt = raw.get("prompt", "")
    if not isinstance(prompt, str):
        raise ConfigError(f"{label}.prompt must be a string")
    return CascadeSettings(
        name=name,
        stt=_stt(raw["stt"], f"{label}.stt", base),
        llm=_llm(raw["llm"], f"{label}.llm", base),
        tts=_tts(raw["tts"], f"{label}.tts", base),
        language=language,
        filler=filler.strip(),
        turn=_turn(raw.get("turn", {}), f"{label}.turn", base),
        barge_in=_barge_in(raw.get("barge_in", {}), f"{label}.barge_in", language),
        playout_lead_ms=_integer(raw, "playout_lead_ms", label, 200, 40, 2000),
        prompt=prompt.strip(),
    )
