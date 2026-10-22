"""Exact matching of names, numbers, dates and addresses in German text.

What a caller said must survive STT, the voice's reply and the request the
back office gets. These functions decide, without an LLM, whether a value is
in a piece of text, however STT or the voice wrote it: "030 23125 417",
"null drei null, zwei drei eins zwei fünf, vier siebzehn" and "03023125417"
are the same number; "13.11.", "dreizehnten November" and "13. November" the
same date; "K wie Kaufmann, O, W …" spells a name.
"""

from __future__ import annotations

import datetime as dt
import re
import unicodedata
from dataclasses import dataclass

from tests.voice_bench.evalset.spec import Value

# -- words and numbers -------------------------------------------------------------

_TOKEN = re.compile(r"[a-zäöüß]+|\d+|[+@]", re.IGNORECASE)

_UNITS = {
    "null": 0,
    "eins": 1,
    "zwei": 2,
    "zwo": 2,
    "drei": 3,
    "vier": 4,
    "fünf": 5,
    "sechs": 6,
    "sieben": 7,
    "acht": 8,
    "neun": 9,
}
_SPECIAL = {
    **_UNITS,
    "zehn": 10,
    "elf": 11,
    "zwölf": 12,
    "dreizehn": 13,
    "vierzehn": 14,
    "fünfzehn": 15,
    "sechzehn": 16,
    "siebzehn": 17,
    "achtzehn": 18,
    "neunzehn": 19,
}
_TENS = {
    "zwanzig": 20,
    "dreißig": 30,
    "dreissig": 30,
    "vierzig": 40,
    "fünfzig": 50,
    "sechzig": 60,
    "siebzig": 70,
    "achtzig": 80,
    "neunzig": 90,
}
#: Unit forms inside a compound ("einundzwanzig", "einhundert").
_COMPOUND_UNITS = {**_UNITS, "ein": 1, "eine": 1, "sech": 6, "sieb": 7}
_ORDINAL_STEMS = {"erst": 1, "dritt": 3, "siebt": 7, "acht": 8}
_ORDINAL_SUFFIXES = ("en", "er", "es", "em", "e")
_MONTHS = {
    "januar": 1,
    "jänner": 1,
    "februar": 2,
    "märz": 3,
    "april": 4,
    "mai": 5,
    "juni": 6,
    "juli": 7,
    "august": 8,
    "september": 9,
    "oktober": 10,
    "november": 11,
    "dezember": 12,
}
WEEKDAYS = (
    "montag",
    "dienstag",
    "mittwoch",
    "donnerstag",
    "freitag",
    "samstag",
    "sonntag",
)
#: Words between a corrected group and its correction ("vier sieben, nein,
#: vier acht"): the group before them is dropped.
_CORRECTIONS = {"nein", "korrektur", "quatsch", "pardon", "sorry", "entschuldigung"}
#: Fillers nobody expects in a transcript.
FILLERS = {"äh", "ähm", "öhm", "ehm", "hm", "hmm", "äähm", "ähem"}


def tokens(text: str) -> list[str]:
    return [t.lower() for t in _TOKEN.findall(text or "")]


def fold(text: str) -> str:
    """Lower case with umlauts spelt out (ä → ae, ß → ss), accents dropped."""
    text = (text or "").lower()
    for a, b in (("ä", "ae"), ("ö", "oe"), ("ü", "ue"), ("ß", "ss")):
        text = text.replace(a, b)
    text = unicodedata.normalize("NFKD", text)
    return "".join(c for c in text if not unicodedata.combining(c))


def compact(text: str) -> str:
    """Folded letters and digits only."""
    return re.sub(r"[^a-z0-9]", "", fold(text))


def cardinal(word: str) -> int | None:
    """A German number word as an int ("dreihundertvierundzwanzig" → 324)."""
    word = word.lower()
    if not word:
        return None
    if word in _SPECIAL:
        return _SPECIAL[word]
    if word in _TENS:
        return _TENS[word]
    for scale_word, scale in (("tausend", 1000), ("hundert", 100)):
        if scale_word in word:
            left, _, right = word.partition(scale_word)
            factor = 1 if left in ("", "ein", "eins") else cardinal(left)
            rest = 0 if not right else cardinal(right.removeprefix("und"))
            if factor is None or rest is None or factor >= scale or rest >= scale:
                return None
            return factor * scale + rest
    if "und" in word:
        unit, _, tens = word.partition("und")
        if unit in _COMPOUND_UNITS and tens in _TENS and _COMPOUND_UNITS[unit] > 0:
            return _TENS[tens] + _COMPOUND_UNITS[unit]
    return None


def ordinal(word: str) -> int | None:
    """A German ordinal ("dreizehnten" → 13, "erste" → 1, "zu fünft" → 5)."""
    word = word.lower()
    for suffix in _ORDINAL_SUFFIXES + ("",):
        if suffix and not word.endswith(suffix):
            continue
        stem = word[: len(word) - len(suffix)] if suffix else word
        # "erste", "dritten"; bare stems only as in "zu dritt" ("erst" is
        # an adverb).
        if stem in _ORDINAL_STEMS and (suffix or stem in ("dritt", "siebt")):
            return _ORDINAL_STEMS[stem]
        if stem.endswith("t"):
            value = cardinal(stem[:-1])
            if value is None and stem.endswith("st"):
                value = cardinal(stem[:-2])
            if value is not None and value > 0:
                return value
    return None


@dataclass(frozen=True)
class Num:
    """A number in the text: its digits as said (leading zeros kept)."""

    digits: str
    ordinal: bool = False
    position: int = 0  # token index

    @property
    def value(self) -> int:
        return int(self.digits)


def numbers(text: str) -> list[Num]:
    """Every number in the text, in order: digit groups as written, number
    words as their value (the separate words of "null drei null" are three
    numbers, "dreiundzwanzig" is one)."""
    out: list[Num] = []
    raw = re.findall(r"\d+\.(?=\s|$)|\d+|[a-zäöüß]+", (text or "").lower())
    for index, token in enumerate(raw):
        if token[0].isdigit():
            if token.endswith("."):
                out.append(Num(token[:-1], True, index))
            else:
                out.append(Num(token, False, index))
            continue
        value = cardinal(token)
        if value is not None and token not in ("ein", "eine"):
            out.append(Num(str(value), False, index))
            continue
        value = ordinal(token)
        if value is not None:
            out.append(Num(str(value), True, index))
    return out


def digit_stream(text: str) -> str:
    """All digits the text says, in order, as one string. A correction drops
    the group said just before it: in "vier sieben, nein, vier acht" the
    stream ends in "48" (groups end at punctuation and correction words)."""
    raw = re.findall(r"\d+|[a-zäöüß]+|[,;.!?]", (text or "").lower())
    chunks: list[list[str]] = [[]]
    for token in raw:
        if token[0].isdigit():
            chunks[-1].append(token)
        elif token in _CORRECTIONS:
            # Drop the last group that said any digits.
            while len(chunks) > 1 and not chunks[-1]:
                chunks.pop()
            chunks[-1] = []
            chunks.append([])
        elif token in ",;.!?":
            if chunks[-1]:
                chunks.append([])
        else:
            value = cardinal(token)
            if value is not None and token not in ("ein", "eine"):
                chunks[-1].append(str(value))
    return "".join("".join(c) for c in chunks)


def phone_digits(value: str) -> tuple[str, ...]:
    """The forms a phone number may take: national ("030…") and international
    without the plus ("4930…")."""
    digits = re.sub(r"\D", "", value)
    if value.strip().startswith("+"):
        national = "0" + digits[2:] if digits.startswith("49") else digits
        return (national, digits)
    if digits.startswith("0"):
        return (digits, "49" + digits[1:])
    return (digits,)


# -- dates and times -------------------------------------------------------------------


def dates(text: str) -> set[tuple[int, int]]:
    """(day, month) pairs: "13.11.", "13. November", "dreizehnten November"."""
    low = (text or "").lower()
    numeric = re.compile(r"\b(\d{1,2})\.\s?(\d{1,2})\.?(?:\s?\d{4})?")
    found = {
        (int(d), int(m))
        for d, m in numeric.findall(low)
        if 1 <= int(d) <= 31 and 1 <= int(m) <= 12
    }
    # The numeric dates are done; their parts are not days of a month name.
    low = numeric.sub(lambda m: " " if int(m.group(2)) <= 12 else m.group(0), low)
    nums = numbers(low)
    words = re.findall(r"\d+\.(?=\s|$)|\d+|[a-zäöüß]+", low)
    days = {n.position: n.value for n in nums if 1 <= n.value <= 31}
    # A day takes the next month name, across "bis zum", "und", "oder" and
    # other days ("vom zwölften bis vierzehnten März").
    link = {"bis", "zum", "zur", "und", "oder", "den", "im", "am", "vom"}
    for position, day in days.items():
        for at in range(position + 1, min(len(words), position + 8)):
            if words[at] in _MONTHS:
                found.add((day, _MONTHS[words[at]]))
                break
            if words[at] not in link and at not in days:
                break
    return found


def resolve_relative(phrase: str, today: dt.date) -> dt.date | None:
    """A relative date phrase against ``today`` ("übermorgen", "nächste Woche
    Montag", "in zwei Wochen"). None for phrases it does not know."""
    words = re.findall(r"[a-zäöüß]+", phrase.lower())
    text = " ".join(words)
    if text == "heute":
        return today
    if text == "morgen":
        return today + dt.timedelta(days=1)
    if text == "übermorgen":
        return today + dt.timedelta(days=2)
    days = [w for w in words if w in WEEKDAYS]
    if "woche" in words and ("nächste" in words or "nächsten" in words) and days:
        monday = today - dt.timedelta(days=today.weekday()) + dt.timedelta(days=7)
        return monday + dt.timedelta(days=WEEKDAYS.index(days[0]))
    if text.startswith("in zwei wochen"):
        base = today + dt.timedelta(days=14)
        if days:
            monday = base - dt.timedelta(days=base.weekday())
            return monday + dt.timedelta(days=WEEKDAYS.index(days[0]))
        return base
    if days and any(w in words for w in ("kommenden", "kommender", "diesen")):
        ahead = (WEEKDAYS.index(days[0]) - today.weekday()) % 7 or 7
        return today + dt.timedelta(days=ahead)
    return None


def relative_phrases(phrase: str) -> list[str]:
    """Ways STT or the voice may write a relative phrase, as folded strings."""
    words = re.findall(r"[a-zäöüß]+", phrase.lower())
    variants = {" ".join(words)}
    days = [w for w in words if w in WEEKDAYS]
    if "woche" in words and days:
        day = days[0]
        variants |= {
            f"nächste woche {day}",
            f"nächsten woche {day}",
            f"{day} nächste woche",
            f"{day} nächster woche",
            f"nächsten {day}",
            f"kommenden {day}",
        }
    return sorted(fold(v) for v in variants)


def times(text: str) -> set[tuple[int, int]]:
    """(hour, minute) pairs: "19:30", "19.30 Uhr", "19 Uhr 30", "halb acht",
    "viertel nach sieben", "neunzehn Uhr dreißig"."""
    low = (text or "").lower()
    found = {
        (int(h), int(m))
        for h, m in re.findall(r"\b(\d{1,2})[:.](\d{2})\b", low)
        if int(h) < 24 and int(m) < 60
    }
    words = re.findall(r"\d+|[a-zäöüß]+", low)

    def num(word: str) -> int | None:
        if word.isdigit():
            return int(word)
        return cardinal(word)

    for index, word in enumerate(words):
        nxt = words[index + 1] if index + 1 < len(words) else ""
        after = words[index + 2] if index + 2 < len(words) else ""
        if word == "uhr" and index > 0 and (hour := num(words[index - 1])) is not None:
            minute = num(nxt) if nxt else None
            if hour < 24:
                found.add((hour, minute if minute is not None and minute < 60 else 0))
        elif word == "halb" and (hour := num(nxt)) is not None and 1 <= hour <= 24:
            found.add(((hour - 1) % 24, 30))
        elif word == "viertel" and nxt in ("nach", "vor"):
            hour = num(after)
            if hour is not None and hour < 24:
                found.add((hour, 15) if nxt == "nach" else ((hour - 1) % 24, 45))
        elif word == "dreiviertel" and (hour := num(nxt)) is not None and hour < 25:
            found.add(((hour - 1) % 24, 45))
    return found


# -- names and spelling ----------------------------------------------------------------

#: German letter names as STT may write them ("Ka", "Zett", "Ypsilon").
_LETTER_NAMES = {
    "ah": "a",
    "be": "b",
    "ce": "c",
    "ze": "c",
    "zeh": "c",
    "de": "d",
    "ef": "f",
    "eff": "f",
    "ge": "g",
    "ha": "h",
    "jot": "j",
    "ka": "k",
    "el": "l",
    "ell": "l",
    "em": "m",
    "en": "n",
    "pe": "p",
    "ku": "q",
    "er": "r",
    "es": "s",
    "te": "t",
    "vau": "v",
    "we": "w",
    "ix": "x",
    "ypsilon": "y",
    "zett": "z",
    "eszett": "ß",
}


def spelled_runs(text: str) -> list[str]:
    """Runs of spelled letters ("K wie Kaufmann, O, W, A …" → "kowa…")."""
    words = re.findall(r"[a-zäöüß]+", (text or "").lower())
    runs: list[str] = []
    current: list[str] = []
    named = 0  # letter names in the run ("ka", "zett"), which are also words
    index = 0
    while index < len(words):
        word = words[index]
        letter = word if len(word) == 1 else _LETTER_NAMES.get(word)
        if letter is not None:
            current.append(letter)
            named += len(word) > 1
            # "K wie Kaufmann": the spelling word is not part of the name.
            if index + 2 < len(words) and words[index + 1] == "wie":
                index += 2
            index += 1
            continue
        if len(current) >= 3 and named < len(current):
            runs.append("".join(current))
        current, named = [], 0
        index += 1
    if len(current) >= 3 and named < len(current):
        runs.append("".join(current))
    return [fold(r) for r in runs]


def name_in(name: str, text: str) -> bool:
    """The name as a word, spelled out, or (a long or double name) with its
    parts run together or split by STT ("Quandt Reiter", "Wojcie chowski")."""
    parts = re.findall(r"[a-z]+", fold(name))
    words = {fold(w) for w in re.findall(r"[a-zäöüß]+", (text or "").lower())}
    if len(parts) == 1 and parts[0] in words:
        return True
    if len(parts) > 1 and all(p in words for p in parts):
        return True
    target = "".join(parts)
    if any(target == run or target in run for run in spelled_runs(text)):
        return True
    return len(target) >= 6 and target in compact(text)


# -- e-mail and addresses ------------------------------------------------------------

_SPOKEN_SYMBOLS = {
    "at": "@",
    "ät": "@",
    "et": "@",
    "punkt": ".",
    "dot": ".",
    "minus": "-",
    "bindestrich": "-",
    "strich": "-",
    "unterstrich": "_",
}


def spoken_email(text: str) -> str:
    """The text as an address would be typed: spoken symbols as characters,
    number words as digits, no spaces ("jana punkt probe at beispiel punkt
    example", "reisen unterstrich zwanzig sechsundzwanzig")."""
    out = []
    for word in re.findall(r"[a-zäöüß0-9]+|[@._\-]", (text or "").lower()):
        if word in _SPOKEN_SYMBOLS:
            out.append(_SPOKEN_SYMBOLS[word])
        elif (number := cardinal(word)) is not None and word not in ("ein", "eine"):
            out.append(str(number))
        else:
            out.append(word)
    return fold("".join(out))


def email_in(address: str, text: str) -> bool:
    return fold(address.lower()) in spoken_email(text)


def address_in(address: str, text: str) -> bool:
    """Street, house number, postcode and town all present."""
    parts = [p.strip() for p in address.split(",")]
    street_part = parts[0]
    town_part = parts[1] if len(parts) > 1 else ""
    street_words = re.findall(r"[a-zäöüß]+", street_part.lower())
    house = re.findall(r"\d+", street_part)
    postcode = re.findall(r"\d+", town_part)
    town_words = re.findall(r"[a-zäöüß]+", town_part.lower())
    folded = compact(text)
    if not all(fold(w) in folded for w in street_words + town_words):
        return False
    stream = digit_stream(text)
    return all(d in stream for d in house + postcode)


# -- one value in one text ---------------------------------------------------------------


@dataclass(frozen=True)
class Match:
    ok: bool
    how: str = ""  # which form matched ("phrase", "date", "spelled", …)


def keyword_in(group: str, text: str) -> bool:
    said = compact(text)
    return any(compact(k) and compact(k) in said for k in group.split("|"))


def match(value: Value, text: str, today: dt.date | None = None) -> Match:
    """Whether ``value`` is in ``text``, by exact rules only."""
    kind, want = value.kind, value.value
    if not text:
        return Match(False)
    if kind == "name":
        if name_in(want, text):
            spelled = any(fold(want) in run for run in spelled_runs(text))
            return Match(True, "spelled" if spelled else "word")
        return Match(False)
    if kind == "phone":
        stream = digit_stream(text)
        return Match(any(form in stream for form in phone_digits(want)), "digits")
    if kind == "number":
        target = int(want)
        return Match(any(n.value == target for n in numbers(text)), "number")
    if kind == "date":
        absolute = parse_date(want)
        found = dates(text)
        if absolute is not None:
            return Match((absolute.day, absolute.month) in found, "date")
        folded = " " + fold(" ".join(re.findall(r"[a-zäöüß]+", text.lower()))) + " "
        if any(f" {p} " in folded for p in relative_phrases(want)):
            return Match(True, "phrase")
        resolved = resolve_relative(want, today) if today else None
        if resolved is not None and (resolved.day, resolved.month) in found:
            return Match(True, "resolved")
        return Match(False)
    if kind == "weekday":
        resolved = resolve_relative(want, today) if today else None
        if resolved is None:
            return Match(False, "undecided")
        day = fold(WEEKDAYS[resolved.weekday()])
        words = {fold(w) for w in re.findall(r"[a-zäöüß]+", text.lower())}
        return Match(day in words, "weekday")
    if kind == "time":
        hour, minute = (int(x) for x in want.split(":"))
        found = times(text)
        ok = (hour, minute) in found or ((hour % 12), minute) in {
            (h % 12, m) for h, m in found
        }
        return Match(ok, "time")
    if kind == "email":
        return Match(email_in(want, text), "email")
    if kind == "address":
        return Match(address_in(want, text), "address")
    if kind == "keyword":
        return Match(keyword_in(want, text), "keyword")
    if kind == "meaning":
        if value.keywords and all(keyword_in(k, text) for k in value.keywords):
            return Match(True, "keywords")
        return Match(False, "undecided")
    raise ValueError(f"unknown value kind {kind}")


def parse_date(value: str) -> dt.date | None:
    """An absolute date in a value: ``2026-11-13`` or ``13.11.`` (year 2000
    stands for any year)."""
    value = value.strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        return dt.date.fromisoformat(value)
    m = re.fullmatch(r"(\d{1,2})\.(\d{1,2})\.(\d{4})?", value)
    if m:
        return dt.date(int(m.group(3) or 2000), int(m.group(2)), int(m.group(1)))
    return None


def is_known_date(value: str) -> bool:
    """A date value the matcher understands (absolute or a known phrase)."""
    if parse_date(value) is not None:
        return True
    return resolve_relative(value, dt.date(2026, 1, 1)) is not None


def strip_fillers(text: str) -> str:
    """The text without "ähm" and friends (for word error rates)."""
    kept = [
        w
        for w in re.findall(r"\S+", text or "")
        if re.sub(r"[^\wäöüß]", "", w.lower()) not in FILLERS
    ]
    return " ".join(kept)
