"""Where a long first sentence may be cut for TTS; knowledge-base/knowledge/features/local_voice_gateway.md §4.3 (WP4).

The TTS gets one request per sentence, so a long first sentence keeps the
caller waiting until the LLM has written all of it. ``clause_end`` finds an
earlier place to start speaking: after a comma, semicolon, colon or dash
once ``min_chars`` are written (the TTS reads the trailing comma as "more
follows", so the voice does not drop its pitch there), or before a
coordinating conjunction ("und", "oder", …) once twice that many are. Only
the first piece of a reply is cut: the rest is synthesised while the first
plays. No Pipecat here, so the main suite tests it.
"""

from __future__ import annotations

#: Punctuation after which a clause ends (followed by a space).
CLAUSE_MARKS = frozenset(",;:–—")
#: Coordinating conjunctions a clause may start with (no comma before them
#: in German); the cut goes before the word.
CONJUNCTIONS = frozenset({"und", "oder", "aber", "sondern", "denn", "sowie"})


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
    if word.lower() in CONJUNCTIONS and len(head.rstrip(",")) >= 2 * min_chars:
        return len(head)
    return None
