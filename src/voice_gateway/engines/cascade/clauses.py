"""Where a long first sentence may be cut for TTS; knowledge-base/knowledge/features/local_voice_gateway.md §4.3 (WP4).

The TTS gets one request per sentence, so a long first sentence keeps the
caller waiting until the LLM has written all of it. ``clause_end`` finds an
earlier place to start speaking: after a comma, semicolon, colon or dash
once ``min_chars`` are written (the TTS reads the trailing comma as "more
follows", so the voice does not drop its pitch there), or before a
coordinating conjunction ("und", "oder", …) once half as many again are. Only
the first piece of a reply is cut: the rest is synthesised while the first
plays. ``sentences`` splits fixed text (the greeting) the same way, so a TTS
that answers only once a request is synthesised starts with the first
sentence. No Pipecat here, so the main suite tests it.
"""

from __future__ import annotations

import re

#: Punctuation after which a clause ends (followed by a space).
CLAUSE_MARKS = frozenset(",;:–—")
#: Coordinating conjunctions a clause may start with (no comma before them
#: in German); the cut goes before the word.
CONJUNCTIONS = frozenset({"und", "oder", "aber", "sondern", "denn", "sowie"})
#: Where a sentence may end: ., ! or ? before a space and a capital.
_SENTENCE_END = re.compile(r"[.!?]\s+(?=[A-ZÄÖÜ„\"])")
#: Words whose period ends no sentence: numbers ("am 12. März"), single
#: letters ("z. B.") and common short forms.
_ABBREVIATIONS = frozenset(
    {"nr", "dr", "hr", "fr", "st", "str", "ca", "bzw", "usw", "inkl", "evtl", "ggf"}
)


def clause_end(text: str, min_chars: int) -> int | None:
    """The length of ``text``'s leading clause, if it may be spoken already.

    ``text`` is what the LLM has written of the sentence so far and ends
    where a word just ended (the last character is a space). Returns where
    the clause ends (the text to speak is ``text[:end]``) or None to wait.
    """
    if min_chars <= 0 or not text.endswith(" "):
        return None
    body = text.rstrip()
    if len(body) < min_chars:
        return None
    last = body[-1]
    if last in CLAUSE_MARKS or (last == "-" and body[-2:-1] == " "):
        return len(body)
    # "…, und" / "… Zimmer und": cut before the conjunction.
    head, _, word = body.rpartition(" ")
    if word.lower() in CONJUNCTIONS and 2 * len(head.rstrip(",")) >= 3 * min_chars:
        return len(head)
    return None


def sentences(text: str) -> list[str]:
    """Fixed text in sentences, for one TTS request each."""
    parts, start = [], 0
    for match in _SENTENCE_END.finditer(text):
        word = text[start : match.start()].rsplit(None, 1)[-1:] or [""]
        word = word[0].lower()
        if text[match.start()] == "." and (
            word.isdigit() or len(word) == 1 or word in _ABBREVIATIONS
        ):
            continue
        parts.append(text[start : match.start() + 1].strip())
        start = match.end()
    parts.append(text[start:].strip())
    return [p for p in parts if p]
