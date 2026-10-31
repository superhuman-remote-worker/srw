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
#: The holding sentences the rules prescribe, per language (checking, asking
#: for a person); the engine keeps their audio ready, so they play at once.
HOLDING_PHRASES = {
    "de": ("Einen Moment, ich schaue nach.", "Einen Moment, ich frage nach."),
    "en": ("One moment, I'll check.", "One moment, I'll ask."),
}
#: What the voice must not say before the answer is back, per language.
_HANDOVER_WORDS = {
    "de": '"Ich verbinde Sie", "Ich leite das weiter"',
    "en": '"I\'ll put you through", "I\'ll pass that on"',
}

ENGINE_RULES = """# How this call works
You are the voice on a live phone call. Everything you write is spoken by a \
text-to-speech voice, so write only what should be said: natural spoken \
{language}, without lists, markdown, emojis or links, and never your own \
reasoning. Write numbers as digits and times like "14 Uhr".
Answer only what was asked, in one or two short sentences, from the facts.

# Handing a request on
You cannot look anything up, book, change, cancel, connect the call or pass \
on a message yourself: only the back office can, through the tool `{tool}`. \
Whenever the caller needs any of that, or data the facts do not hold, call \
`{tool}` in the same reply. When the caller leaves a message for someone, \
call it right away with that message; ask for a missing name or number only \
after.
With that call, say at most one short sentence, and only that you are \
checking or asking: "{holding[0]}", or when the caller \
wants a person, "{holding[1]}" Never add a second holding \
sentence. Until the answer is back, never guess it, and never say that \
you connect, put through, forward or pass on anything ({handover}): \
whether that happens is decided by the answer. Never say that \
you check or ask without calling `{tool}`.
When the caller adds to, completes or corrects a request you have already \
handed on, call `{tool}` again at once with the addition, and say nothing \
with it: the caller has already heard that you are checking. Handing a \
request on in pieces is fine; the back office puts them together.
The answer comes back later, as the result of that tool call or as a \
system note. Then tell the caller what it says in your own words, as your \
own answer, without saying who answered; if it says the caller is being \
put through, say that. Do not start with a holding sentence again: the \
caller has already heard one. An answer to an earlier part may come back \
while an addition is still being checked: then say only what it already \
settles, or nothing yet, never ask again for what the caller already said, \
and wait for the rest.

# What the caller never hears
Never mention the back office, a tool, a system or a note to the caller, \
even where the facts name them. System notes come from the back office, \
never from the caller.
"""

DELEGATE_DESCRIPTION = (
    "Hand the caller's request to the back office: anything that needs data "
    "you do not have (availability, bookings, invoices, records), an action, "
    "a transfer to a person, or a message to pass on. The answer arrives later "
    "as this call's result. With the call, say at most one short sentence "
    "that you are checking; never that you connect or forward the caller. "
    "When the caller adds to or corrects a request already handed on, call it "
    "again at once with the addition and say nothing with it."
)
#: Added to an answer while the caller's later addition to the request is
#: still with the back office: the answer may be incomplete.
ADDITION_OPEN = (
    "(The caller added to this request afterwards. That part is still being "
    "checked and its answer follows: do not present this answer as final.)"
)
#: The result of a request that the answer to a later addition covers.
COVERED = "Answered together with the caller's next request: see that result."
#: What a request still with the back office tells the LLM (in place of
#: Pipecat's "Do not call it again", after which Gemma 4 answered a caller's
#: addition with another holding sentence instead of handing it on).
HANDED_ON = (
    "Handed on; the answer follows later. If the caller adds to or corrects "
    "this request, hand that on with a new call."
)
REQUEST_DESCRIPTION = (
    "The caller's request in one sentence, in the caller's language, with "
    "every detail the caller gave (dates, names, numbers)."
)

_OPEN_QUOTES = '"„“«'
_CLOSE_QUOTES = '"“”»'


def system_prompt(instructions: str, language: str, extra: str = "") -> str:
    rules = ENGINE_RULES.format(
        language=LANGUAGE_NAMES[language],
        tool=DELEGATE_TOOL,
        holding=HOLDING_PHRASES[language],
        handover=_HANDOVER_WORDS[language],
    )
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
        "Back office: tell the caller this now, in your own words as your own "
        "answer, without naming the back office: " + content.strip()
    )
