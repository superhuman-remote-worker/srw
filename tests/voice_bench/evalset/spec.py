"""The test set's text files as data: voices, phone conditions, the caller
corpus, multi-turn scenarios and the slot for real recordings.

Everything here is plain parsing and validation (PyYAML only), so the normal
suite tests it without audio or model servers. The files live in ``de/``;
the audio rendered from them lives in the gitignored cache.

Caller text may carry pauses: ``"von Montag <pause 2.0> bis Mittwoch"`` is
rendered as two pieces with 2 s of silence between them (a caller thinking
mid-sentence). The reference transcript is the text without the markup.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

import yaml

EVALSET_DIR = Path(__file__).resolve().parent
DATA_DIR = EVALSET_DIR / "de"
HUMAN_DIR = EVALSET_DIR / "human"

CATEGORIES = (
    "facts",
    "back_office",
    "transfer",
    "messages",
    "names",
    "phone_numbers",
    "dates_times",
    "addresses",
    "emails",
    "counts",
    "short_answers",
    "backchannels",
    "barge_in",
    "silence",
)
STYLES = (
    "plain",
    "hesitation",
    "pause",
    "self_correction",
    "spelled",
    "grouped",
    "short",
)
#: Value kinds and the category their exact-match results count towards.
VALUE_KINDS = {
    "name": "names",
    "phone": "phone_numbers",
    "date": "dates_times",
    "time": "dates_times",
    "weekday": "dates_times",
    "email": "emails",
    "number": "counts",
    "address": "addresses",
    "keyword": "keywords",
    "meaning": "meaning",
}
#: Where a value must show up: what STT made of the caller (``stt``), the
#: voice's reply in that turn (``reply``), or what the back office got for
#: the delegation (``request``: the caller's words since the last delegation,
#: as SRW's driver forwards them, and the voice LLM's own tool argument).
WHERE = ("stt", "reply", "request", "either")
OUTCOMES = ("answer", "delegate", "transfer", "message", "ignore", "any")
WHEN = ("after_voice", "during_voice", "after_caller")
ENGINES = ("qwen-design", "piper", "human")
#: Operations a phone condition chains; ``acoustic`` ones happen before the
#: phone network (the caller's room and handset), ``line`` ones on it. At call
#: level the bench's real line replaces the codec.
OPS = {
    "noise": "acoustic",
    "level": "acoustic",
    "clip": "acoustic",
    "band": "acoustic",
    "codec": "line",
    "loss": "line",
}
CODECS = ("g711a", "g711u", "g722", "amr-nb")
NOISES = ("street", "cafe", "office", "white")

#: What the canned back office says to a delegation no turn's answer matches.
DEFAULT_ANSWER = "Das ist notiert, wir melden uns dazu bei Ihnen."

_PAUSE = re.compile(r"<pause\s+([0-9]+(?:\.[0-9]+)?)\s*>", re.IGNORECASE)
_MARKUP = re.compile(r"<[^>]*>")


class SpecError(ValueError):
    """A test-set file does not say what the runner needs."""


# -- caller text ---------------------------------------------------------------


@dataclass(frozen=True)
class Segment:
    """A piece of caller speech and the silence after it."""

    text: str
    pause_after: float = 0.0


def segments(text: str) -> list[Segment]:
    """Split caller text at ``<pause S>`` markers."""
    parts = _PAUSE.split(text)
    pieces: list[Segment] = []
    for index in range(0, len(parts), 2):
        words = " ".join(parts[index].split())
        pause = float(parts[index + 1]) if index + 1 < len(parts) else 0.0
        if words:
            pieces.append(Segment(words, pause))
        elif pieces and pause:
            last = pieces[-1]
            pieces[-1] = Segment(last.text, last.pause_after + pause)
    leftover = _MARKUP.search(_PAUSE.sub("", text))
    if leftover:
        raise SpecError(f"unknown markup {leftover.group(0)!r} in {text!r}")
    for piece in pieces:
        if not 0 <= piece.pause_after <= 10:
            raise SpecError(f"pause out of range in {text!r}")
    return pieces


def reference(text: str) -> str:
    """The words a perfect STT would write: the text without markup."""
    return " ".join(" ".join(s.text for s in segments(text)).split())


def has_pause(text: str, at_least: float = 1.0) -> bool:
    return any(s.pause_after >= at_least for s in segments(text)[:-1])


# -- values --------------------------------------------------------------------


@dataclass(frozen=True)
class Value:
    """Something the caller said that must survive: a name, a number, a date."""

    kind: str
    value: str
    where: str | None = None  # None: decided by the turn's expected outcome
    #: For ``meaning``: words an exact pass tries before the LLM judge.
    keywords: tuple[str, ...] = ()

    @property
    def category(self) -> str:
        return VALUE_KINDS[self.kind]

    def label(self) -> str:
        return f"{self.kind}:{self.value}"


def parse_value(raw, label: str) -> Value:
    if not isinstance(raw, dict):
        raise SpecError(f"{label}: a value is a mapping like {{name: Müller}}")
    kinds = [k for k in raw if k in VALUE_KINDS]
    if len(kinds) != 1:
        raise SpecError(f"{label}: name exactly one of {', '.join(VALUE_KINDS)}")
    unknown = set(raw) - {kinds[0], "in", "keywords"}
    if unknown:
        raise SpecError(f"{label}: unknown keys {sorted(unknown)}")
    kind = kinds[0]
    value = raw[kind]
    if isinstance(value, bool) or value is None or str(value).strip() == "":
        raise SpecError(f"{label}: {kind} needs a value")
    where = raw.get("in")
    if where is not None and where not in WHERE:
        raise SpecError(f"{label}: in must be one of {', '.join(WHERE)}")
    keywords = raw.get("keywords") or []
    if not isinstance(keywords, list):
        raise SpecError(f"{label}: keywords must be a list")
    return Value(kind, str(value).strip(), where, tuple(str(k) for k in keywords))


def parse_values(raw, label: str) -> tuple[Value, ...]:
    if raw is None:
        return ()
    if not isinstance(raw, list):
        raise SpecError(f"{label} must be a list")
    return tuple(parse_value(item, f"{label}[{i}]") for i, item in enumerate(raw))


# -- voices and conditions -------------------------------------------------------


@dataclass(frozen=True)
class Voice:
    id: str
    engine: str
    gender: str = ""
    age: str = ""
    pace: str = ""
    #: Qwen3-TTS VoiceDesign: the description and the seed of the design take.
    instruct: str = ""
    seed: int = 0
    #: Pitch-preserving speed factor applied after synthesis (1.0 = as made).
    tempo: float = 1.0
    #: Piper: the speaker id of ``de_DE-mls-medium``.
    speaker: int | None = None
    #: Harder than the others on purpose (a robotic or garbling voice).
    hard: bool = False


@dataclass(frozen=True)
class VoiceSet:
    language: str
    reference_text: str
    voices: dict[str, Voice]
    #: The voices the corpus rotates through when an utterance names none.
    rotation: tuple[str, ...]


def parse_voices(raw: dict) -> VoiceSet:
    if not isinstance(raw, dict) or not isinstance(raw.get("voices"), list):
        raise SpecError("voices.yaml needs a voices list")
    voices: dict[str, Voice] = {}
    for index, item in enumerate(raw["voices"]):
        label = f"voices[{index}]"
        if not isinstance(item, dict) or not item.get("id"):
            raise SpecError(f"{label} needs an id")
        engine = item.get("engine")
        if engine not in ENGINES or engine == "human":
            raise SpecError(f"{label}.engine must be qwen-design or piper")
        allowed = {
            "id",
            "engine",
            "gender",
            "age",
            "pace",
            "instruct",
            "seed",
            "tempo",
            "speaker",
            "hard",
        }
        unknown = set(item) - allowed
        if unknown:
            raise SpecError(f"{label}: unknown keys {sorted(unknown)}")
        tempo = float(item.get("tempo", 1.0))
        if not 0.5 <= tempo <= 2.0:
            raise SpecError(f"{label}.tempo must be between 0.5 and 2.0")
        if engine == "qwen-design":
            if not item.get("instruct") or type(item.get("seed")) is not int:
                raise SpecError(f"{label}: a designed voice needs instruct and seed")
        if engine == "piper" and type(item.get("speaker")) is not int:
            raise SpecError(f"{label}: a Piper voice needs a speaker id")
        voice = Voice(
            id=str(item["id"]),
            engine=engine,
            gender=str(item.get("gender", "")),
            age=str(item.get("age", "")),
            pace=str(item.get("pace", "")),
            instruct=str(item.get("instruct", "")).strip(),
            seed=int(item.get("seed", 0)),
            tempo=tempo,
            speaker=item.get("speaker"),
            hard=bool(item.get("hard", False)),
        )
        if voice.id in voices:
            raise SpecError(f"duplicate voice {voice.id}")
        voices[voice.id] = voice
    rotation = tuple(raw.get("rotation") or [])
    for name in rotation:
        if name not in voices:
            raise SpecError(f"rotation names an unknown voice {name}")
    if not rotation:
        raise SpecError("voices.yaml needs a rotation")
    reference_text = str(raw.get("reference_text", "")).strip()
    if not reference_text:
        raise SpecError("voices.yaml needs the reference_text designed voices say")
    return VoiceSet(
        str(raw.get("language", "German")), reference_text, voices, rotation
    )


@dataclass(frozen=True)
class Op:
    op: str
    params: dict

    @property
    def stage(self) -> str:
        return OPS[self.op]


@dataclass(frozen=True)
class Condition:
    id: str
    description: str
    ops: tuple[Op, ...]

    def stage(self, stage: str) -> Condition:
        """Only the operations of one stage (``acoustic`` or ``line``)."""
        return Condition(
            self.id, self.description, tuple(o for o in self.ops if o.stage == stage)
        )

    @property
    def codec(self) -> str | None:
        return next((o.params["name"] for o in self.ops if o.op == "codec"), None)


_OP_PARAMS = {
    "noise": {"kind", "snr_db", "seed"},
    "level": {"db"},
    "clip": {"gain_db"},
    "band": {"low", "high"},
    "codec": {"name", "bitrate"},
    "loss": {"rate", "burst", "plc", "seed"},
}


def parse_op(raw, label: str) -> Op:
    if not isinstance(raw, dict) or raw.get("op") not in OPS:
        raise SpecError(f"{label}: op must be one of {', '.join(OPS)}")
    name = raw["op"]
    params = {k: v for k, v in raw.items() if k != "op"}
    unknown = set(params) - _OP_PARAMS[name]
    if unknown:
        raise SpecError(f"{label}: {name} does not take {sorted(unknown)}")
    if name == "noise":
        if params.get("kind") not in NOISES:
            raise SpecError(f"{label}: noise kind must be one of {', '.join(NOISES)}")
        if not isinstance(params.get("snr_db"), int | float):
            raise SpecError(f"{label}: noise needs snr_db")
    elif name == "level" and not isinstance(params.get("db"), int | float):
        raise SpecError(f"{label}: level needs db")
    elif name == "clip" and not isinstance(params.get("gain_db"), int | float):
        raise SpecError(f"{label}: clip needs gain_db")
    elif name == "band":
        low, high = params.get("low"), params.get("high")
        if not (isinstance(low, int | float) and isinstance(high, int | float)):
            raise SpecError(f"{label}: band needs low and high (Hz)")
        if not 0 <= low < high <= 8000:
            raise SpecError(f"{label}: band must satisfy 0 <= low < high <= 8000")
    elif name == "codec" and params.get("name") not in CODECS:
        raise SpecError(f"{label}: codec must be one of {', '.join(CODECS)}")
    elif name == "loss":
        rate = params.get("rate")
        if not isinstance(rate, int | float) or not 0 <= rate < 0.5:
            raise SpecError(f"{label}: loss rate must be in [0, 0.5)")
        if params.get("plc", "zero") not in ("zero", "repeat"):
            raise SpecError(f"{label}: plc must be zero or repeat")
        burst = params.get("burst", 1.0)
        if not isinstance(burst, int | float) or burst < 1:
            raise SpecError(f"{label}: burst (mean lost frames in a row) must be >= 1")
    return Op(name, params)


def parse_conditions(raw: dict) -> dict[str, Condition]:
    if not isinstance(raw, dict) or not isinstance(raw.get("conditions"), list):
        raise SpecError("conditions.yaml needs a conditions list")
    out: dict[str, Condition] = {}
    for index, item in enumerate(raw["conditions"]):
        label = f"conditions[{index}]"
        if not isinstance(item, dict) or not item.get("id"):
            raise SpecError(f"{label} needs an id")
        ops = item.get("ops") or []
        if not isinstance(ops, list):
            raise SpecError(f"{label}.ops must be a list")
        cond = Condition(
            str(item["id"]),
            str(item.get("description", "")),
            tuple(parse_op(o, f"{label}.ops[{i}]") for i, o in enumerate(ops)),
        )
        codecs = [o for o in cond.ops if o.op == "codec"]
        if len(codecs) > 2:
            raise SpecError(f"{label}: at most two codecs (mobile leg, then the line)")
        if cond.id in out:
            raise SpecError(f"duplicate condition {cond.id}")
        out[cond.id] = cond
    return out


# -- the corpus ------------------------------------------------------------------


@dataclass(frozen=True)
class Utterance:
    id: str
    category: str
    text: str
    styles: tuple[str, ...] = ("plain",)
    values: tuple[Value, ...] = ()
    voice: str | None = None
    #: Rendered with every voice (the voice comparison), not just one.
    sweep: bool = False
    #: A silent clip of this many seconds instead of speech.
    silence: float = 0.0
    note: str = ""

    @property
    def reference(self) -> str:
        return reference(self.text) if self.text else ""


@dataclass(frozen=True)
class Corpus:
    utterances: dict[str, Utterance]
    #: Conditions every utterance is scored under (the file-level STT pass).
    conditions: tuple[str, ...]
    #: Conditions the voice sweep is scored under.
    sweep_conditions: tuple[str, ...]


def parse_utterance(item, label: str) -> Utterance:
    if not isinstance(item, dict) or not item.get("id"):
        raise SpecError(f"{label} needs an id")
    allowed = {
        "id",
        "category",
        "text",
        "style",
        "values",
        "voice",
        "sweep",
        "silence",
        "note",
    }
    unknown = set(item) - allowed
    if unknown:
        raise SpecError(f"{label}: unknown keys {sorted(unknown)}")
    category = item.get("category")
    if category not in CATEGORIES:
        raise SpecError(f"{label}.category must be one of {', '.join(CATEGORIES)}")
    styles = item.get("style", ["plain"])
    styles = [styles] if isinstance(styles, str) else list(styles)
    bad = [s for s in styles if s not in STYLES]
    if bad:
        raise SpecError(f"{label}: unknown style {bad[0]}")
    text = str(item.get("text") or "")
    silence = float(item.get("silence") or 0.0)
    if bool(text) == bool(silence):
        raise SpecError(f"{label} needs either text or silence")
    if text:
        segments(text)  # validates the markup
        if ("pause" in styles) != has_pause(text):
            raise SpecError(f"{label}: style pause and a <pause> of >= 1 s go together")
    return Utterance(
        id=str(item["id"]),
        category=category,
        text=text,
        styles=tuple(styles),
        values=parse_values(item.get("values"), f"{label}.values"),
        voice=item.get("voice"),
        sweep=bool(item.get("sweep", False)),
        silence=silence,
        note=str(item.get("note", "")),
    )


def parse_corpus(
    raw: dict, voices: VoiceSet | None = None, conditions: dict | None = None
) -> Corpus:
    if not isinstance(raw, dict) or not isinstance(raw.get("utterances"), list):
        raise SpecError("corpus.yaml needs an utterances list")
    out: dict[str, Utterance] = {}
    for index, item in enumerate(raw["utterances"]):
        utt = parse_utterance(item, f"utterances[{index}]")
        if utt.id in out:
            raise SpecError(f"duplicate utterance {utt.id}")
        if voices is not None and utt.voice and utt.voice not in voices.voices:
            raise SpecError(f"{utt.id} names an unknown voice {utt.voice}")
        out[utt.id] = utt
    conds = tuple(raw.get("conditions") or ())
    sweep = tuple(raw.get("sweep_conditions") or ())
    if conditions is not None:
        for name in conds + sweep:
            if name not in conditions:
                raise SpecError(f"corpus names an unknown condition {name}")
    if not conds:
        raise SpecError("corpus.yaml needs the conditions it is scored under")
    return Corpus(out, conds, sweep or conds[:1])


def corpus_voice(utt: Utterance, index: int, voices: VoiceSet) -> str:
    """The utterance's own voice, else the rotation's."""
    return utt.voice or voices.rotation[index % len(voices.rotation)]


# -- scenarios -------------------------------------------------------------------


@dataclass(frozen=True)
class BackOffice:
    answer: str
    after: float = 4.0
    transfer: bool = False
    #: Keywords (``a|b`` for either) the caller's words since the last
    #: delegation must hold for this answer; empty matches any request.
    match: tuple[str, ...] = ()
    #: A line to say meanwhile, ``status_after`` seconds in (the back
    #: office's ``Say exactly`` before a slow lookup).
    status: str = ""
    status_after: float = 2.0
    #: SRW's live driver after ``answer_timeout_s`` without an answer: this
    #: text as the delegation's result, ``follow_up_after`` seconds in; the
    #: answer comes later.
    follow_up: str = ""
    follow_up_after: float = 25.0

    def as_driver_entry(self) -> dict:
        """The entry ``srw_driver.py``'s canned back office takes."""
        entry = {
            "keywords": list(self.match),
            "answer": self.answer,
            "delay": self.after,
        }
        if self.transfer:
            entry["transfer"] = True
        if self.status:
            entry["status"] = self.status
            entry["status_delay"] = self.status_after
        if self.follow_up:
            entry["follow_up"] = self.follow_up
            entry["follow_up_delay"] = self.follow_up_after
        return entry


@dataclass(frozen=True)
class Expect:
    outcome: str = "any"
    values: tuple[Value, ...] = ()
    #: Values that must NOT show up (e.g. the date a caller corrected).
    absent: tuple[Value, ...] = ()
    #: The caller talks over the voice on purpose: the voice must stop.
    barge_in: bool = False


@dataclass(frozen=True)
class Turn:
    index: int
    text: str = ""
    utterance: str | None = None
    silence: float = 0.0
    voice: str | None = None
    condition: str | None = None
    when: str = "after_voice"
    #: during_voice: seconds after the voice started; after_caller: seconds
    #: after the previous caller line ended; after_voice: extra quiet seconds.
    offset: float = 0.0
    expect: Expect = field(default_factory=Expect)
    back_office: BackOffice | None = None

    @property
    def label(self) -> str:
        return f"t{self.index + 1}"


@dataclass(frozen=True)
class Scenario:
    id: str
    title: str
    why: str
    categories: tuple[str, ...]
    voice: str
    condition: str
    turns: tuple[Turn, ...]
    number: str = "107"
    default_answer: str = ""

    def back_office(self) -> list[BackOffice]:
        return [t.back_office for t in self.turns if t.back_office is not None]


def _parse_expect(raw, label: str) -> Expect:
    if raw is None:
        return Expect()
    if not isinstance(raw, dict):
        raise SpecError(f"{label} must be a mapping")
    unknown = set(raw) - {"outcome", "values", "absent", "barge_in"}
    if unknown:
        raise SpecError(f"{label}: unknown keys {sorted(unknown)}")
    outcome = raw.get("outcome", "any")
    if outcome not in OUTCOMES:
        raise SpecError(f"{label}.outcome must be one of {', '.join(OUTCOMES)}")
    return Expect(
        outcome=outcome,
        values=parse_values(raw.get("values"), f"{label}.values"),
        absent=parse_values(raw.get("absent"), f"{label}.absent"),
        barge_in=bool(raw.get("barge_in", False)),
    )


def _parse_back_office(raw, label: str) -> BackOffice | None:
    if raw is None:
        return None
    if not isinstance(raw, dict) or not str(raw.get("answer") or "").strip():
        raise SpecError(f"{label} needs an answer")
    unknown = set(raw) - {
        "answer",
        "after",
        "transfer",
        "match",
        "status",
        "status_after",
        "follow_up",
        "follow_up_after",
    }
    if unknown:
        raise SpecError(f"{label}: unknown keys {sorted(unknown)}")
    after = _seconds(raw, "after", 4.0, label)
    match = raw.get("match") or []
    if not isinstance(match, list):
        raise SpecError(f"{label}.match must be a list")
    status_after = _seconds(raw, "status_after", 2.0, label)
    follow_up_after = _seconds(raw, "follow_up_after", 25.0, label)
    for key, at in (
        ("status_after", status_after),
        ("follow_up_after", follow_up_after),
    ):
        if key in raw and at >= after:
            raise SpecError(f"{label}.{key} must come before the answer")
    return BackOffice(
        answer=str(raw["answer"]).strip(),
        after=after,
        transfer=bool(raw.get("transfer", False)),
        match=tuple(str(m).lower() for m in match),
        status=str(raw.get("status") or "").strip(),
        status_after=status_after,
        follow_up=str(raw.get("follow_up") or "").strip(),
        follow_up_after=follow_up_after,
    )


def _seconds(raw: dict, key: str, default: float, label: str) -> float:
    value = raw.get(key, default)
    if not isinstance(value, int | float) or not 0 <= value <= 60:
        raise SpecError(f"{label}.{key} must be 0-60 seconds")
    return float(value)


def parse_scenario(
    raw: dict,
    *,
    corpus: Corpus | None = None,
    voices: VoiceSet | None = None,
    conditions: dict | None = None,
) -> Scenario:
    if not isinstance(raw, dict):
        raise SpecError("a scenario is a mapping")
    allowed = {
        "id",
        "title",
        "why",
        "categories",
        "voice",
        "condition",
        "number",
        "turns",
        "default_answer",
    }
    unknown = set(raw) - allowed
    if unknown:
        raise SpecError(f"unknown scenario keys {sorted(unknown)}")
    sid = str(raw.get("id") or "")
    if not re.fullmatch(r"s\d\d-[a-z0-9-]+", sid):
        raise SpecError(f"scenario id {sid!r} must look like s01-short-name")
    categories = tuple(raw.get("categories") or ())
    bad = [c for c in categories if c not in CATEGORIES]
    if bad:
        raise SpecError(f"{sid}: unknown category {bad[0]}")
    voice = str(raw.get("voice") or "")
    condition = str(raw.get("condition") or "")
    if not voice or not condition:
        raise SpecError(f"{sid} needs a voice and a condition")
    number = str(raw.get("number", "107"))
    if not number.isdigit():
        raise SpecError(f"{sid}: number must be a bench dialplan number")
    turns_raw = raw.get("turns")
    if not isinstance(turns_raw, list) or not turns_raw:
        raise SpecError(f"{sid} needs turns")
    turns = []
    for index, item in enumerate(turns_raw):
        label = f"{sid}.turns[{index}]"
        if not isinstance(item, dict):
            raise SpecError(f"{label} must be a mapping")
        unknown = set(item) - {
            "say",
            "utterance",
            "silence",
            "voice",
            "condition",
            "when",
            "offset",
            "expect",
            "back_office",
        }
        if unknown:
            raise SpecError(f"{label}: unknown keys {sorted(unknown)}")
        sources = [k for k in ("say", "utterance", "silence") if item.get(k)]
        if len(sources) != 1:
            raise SpecError(f"{label} needs exactly one of say, utterance, silence")
        text = str(item.get("say") or "")
        utterance = item.get("utterance")
        if utterance is not None:
            if corpus is not None and utterance not in corpus.utterances:
                raise SpecError(f"{label} names an unknown utterance {utterance}")
            if corpus is not None:
                text = corpus.utterances[utterance].text
        if text:
            segments(text)
        when = item.get("when", "after_voice")
        if when not in WHEN:
            raise SpecError(f"{label}.when must be one of {', '.join(WHEN)}")
        offset = item.get("offset", 0.0)
        if not isinstance(offset, int | float) or not 0 <= offset <= 60:
            raise SpecError(f"{label}.offset must be 0-60 seconds")
        turn_voice = item.get("voice")
        turn_cond = item.get("condition")
        if voices is not None:
            for name in filter(None, (voice if index == 0 else None, turn_voice)):
                if name not in voices.voices:
                    raise SpecError(f"{label} names an unknown voice {name}")
        if conditions is not None:
            for name in filter(None, (condition if index == 0 else None, turn_cond)):
                if name not in conditions:
                    raise SpecError(f"{label} names an unknown condition {name}")
        expect = _parse_expect(item.get("expect"), f"{label}.expect")
        if expect.barge_in and when != "during_voice":
            raise SpecError(f"{label}: barge_in needs when: during_voice")
        turns.append(
            Turn(
                index=index,
                text=text,
                utterance=utterance,
                silence=float(item.get("silence") or 0.0),
                voice=turn_voice,
                condition=turn_cond,
                when=when,
                offset=float(offset),
                expect=expect,
                back_office=_parse_back_office(
                    item.get("back_office"), f"{label}.back_office"
                ),
            )
        )
    return Scenario(
        id=sid,
        title=str(raw.get("title") or sid),
        why=str(raw.get("why") or "").strip(),
        categories=categories,
        voice=voice,
        condition=condition,
        turns=tuple(turns),
        number=number,
        default_answer=str(raw.get("default_answer") or DEFAULT_ANSWER).strip(),
    )


# -- real recordings (the owner's slot; never committed) ----------------------------


@dataclass(frozen=True)
class HumanRecording:
    id: str
    file: Path
    text: str
    speaker: str
    consent: str
    category: str
    values: tuple[Value, ...] = ()
    #: Already recorded through a phone: scored as it is, no condition added.
    phone: bool = False


def parse_human(raw: dict, base: Path) -> list[HumanRecording]:
    if not isinstance(raw, dict) or not isinstance(raw.get("recordings"), list):
        raise SpecError("the human manifest needs a recordings list")
    out = []
    seen = set()
    for index, item in enumerate(raw["recordings"]):
        label = f"recordings[{index}]"
        if not isinstance(item, dict):
            raise SpecError(f"{label} must be a mapping")
        missing = [
            k for k in ("id", "file", "text", "speaker", "consent") if not item.get(k)
        ]
        if missing:
            raise SpecError(f"{label} needs {', '.join(missing)}")
        category = item.get("category", "facts")
        if category not in CATEGORIES:
            raise SpecError(f"{label}.category must be one of {', '.join(CATEGORIES)}")
        rid = f"human-{item['id']}"
        if rid in seen:
            raise SpecError(f"duplicate recording {item['id']}")
        seen.add(rid)
        out.append(
            HumanRecording(
                id=rid,
                file=(base / str(item["file"])).resolve(),
                text=str(item["text"]),
                speaker=str(item["speaker"]),
                consent=str(item["consent"]),
                category=category,
                values=parse_values(item.get("values"), f"{label}.values"),
                phone=bool(item.get("phone", False)),
            )
        )
    return out


# -- loading the files ---------------------------------------------------------------


@dataclass(frozen=True)
class EvalSet:
    voices: VoiceSet
    conditions: dict[str, Condition]
    corpus: Corpus
    scenarios: dict[str, Scenario]
    human: list[HumanRecording]


def _yaml(path: Path) -> dict:
    try:
        return yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as exc:
        raise SpecError(f"{path.name}: {exc}") from exc


def load(data_dir: Path = DATA_DIR, human_dir: Path = HUMAN_DIR) -> EvalSet:
    voices = parse_voices(_yaml(data_dir / "voices.yaml"))
    conditions = parse_conditions(_yaml(data_dir / "conditions.yaml"))
    corpus = parse_corpus(_yaml(data_dir / "corpus.yaml"), voices, conditions)
    scenarios: dict[str, Scenario] = {}
    for path in sorted((data_dir / "scenarios").glob("*.yaml")):
        try:
            scenario = parse_scenario(
                _yaml(path), corpus=corpus, voices=voices, conditions=conditions
            )
        except SpecError as exc:
            raise SpecError(f"{path.name}: {exc}") from exc
        if scenario.id != path.stem:
            raise SpecError(f"{path.name}: id {scenario.id} must match the file name")
        scenarios[scenario.id] = scenario
    manifest = human_dir / "manifest.yaml"
    human = parse_human(_yaml(manifest), human_dir) if manifest.exists() else []
    return EvalSet(voices, conditions, corpus, scenarios, human)
