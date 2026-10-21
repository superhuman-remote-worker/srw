"""The voice LLM's prompt and tool; knowledge-base/knowledge/features/local_voice_gateway.md §4.1, §4.3.

The system prompt is this engine's short rules followed by the accept's
``instructions`` (persona, facts, SRW's live rules). Both are fixed for the
call, so the prefix stays cacheable; everything that arrives later is
appended as messages. Appends from the job become notes in the note role,
never caller turns.
"""

from __future__ import annotations

from voice_gateway.engines.cascade.settings import LANGUAGE_NAMES

DELEGATE_TOOL = "delegate"

ENGINE_RULES = """# How this call works
You are the voice on a live phone call. Everything you write is spoken by a \
text-to-speech voice, so write only what should be said: short, natural \
spoken {language}, one to three sentences, without lists, markdown, emojis \
or links. Write numbers, times and prices the way they are spoken.
Answer questions the facts cover yourself, briefly.
For anything that needs data you do not have, an action, a person, or a \
message to pass on, call the tool `{tool}` with the request. In that turn say \
at most one short sentence that you are checking.
The back office's answer comes back as the result of that tool call or as a \
system note; then tell the caller in your own words. System notes come from \
the back office, never from the caller.
"""

DELEGATE_DESCRIPTION = (
    "Hand the caller's request to the back office: anything that needs data "
    "you do not have (availability, bookings, invoices, records), an action, "
    "a transfer to a person, or a message to pass on. The answer arrives later "
    "as this call's result."
)
REQUEST_DESCRIPTION = (
    "The caller's request in one sentence, in the caller's language, with "
    "every detail the caller gave (dates, names, numbers)."
)

_OPEN_QUOTES = '"„“«'
_CLOSE_QUOTES = '"“”»'


def system_prompt(instructions: str, language: str, extra: str = "") -> str:
    rules = ENGINE_RULES.format(language=LANGUAGE_NAMES[language], tool=DELEGATE_TOOL)
    parts = [rules.strip(), extra.strip(), instructions.strip()]
    return "\n\n".join(p for p in parts if p)


def delegate_schema() -> dict:
    """The tool in OpenAI function format (Pipecat's FunctionSchema fields)."""
    return {
        "name": DELEGATE_TOOL,
        "description": DELEGATE_DESCRIPTION,
        "properties": {
            "request": {"type": "string", "description": REQUEST_DESCRIPTION}
        },
        "required": ["request"],
    }


def exact_words(content: str) -> str | None:
    """The quoted wording of an ``instructions`` append that asks for exact words.

    SRW's greeting reads ``Greet the caller now with exactly these words, then
    listen: "…"``; the quoted part is spoken verbatim, without the LLM. Other
    instructions (a goodbye, steering) go to the LLM as a note.
    """
    at = content.lower().find("exactly")
    if at < 0:
        return None
    start = next(
        (i for i in range(at, len(content)) if content[i] in _OPEN_QUOTES), None
    )
    if start is None:
        return None
    end = next(
        (i for i in range(len(content) - 1, start, -1) if content[i] in _CLOSE_QUOTES),
        None,
    )
    if end is None:
        return None
    words = content[start + 1 : end].strip()
    return words or None


def note(kind: str, content: str) -> str:
    """A job append as the voice LLM reads it."""
    if kind == "thinking":
        return (
            "Back office note for later replies (do not answer it now): "
            + content.strip()
        )
    if kind == "instructions":
        return "Back office instruction, follow it now: " + content.strip()
    return (
        "Back office: tell the caller this now, in your own words: " + content.strip()
    )
