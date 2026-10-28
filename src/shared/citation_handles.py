"""Citation handles: the marker an agent writes where a claim rests on a citation.

The cite tools give the agent a handle, ``[^c1241]``, and the agent copies it
into its text right after the claim. The number is the citation record's id, so
the handle is unique: copied into another job's file or a KB note it can never
point at the wrong record. It is not a display number. Readers see ¹ ² ³,
assigned when the text is rendered (per answer in chat, per document at
delivery), so the model's history is never rewritten.

The handle is Markdown footnote syntax on purpose. Once the delivery pass
appends ``[^c1241]: Source, locator`` definitions, GitHub, Obsidian and Pandoc
number the footnotes by first mention themselves. Sources are written ``S12``,
never in brackets, so a source id is not mistaken for a citation.

Before 2026-10-28 the tools printed the bare id, ``[1241]``. Old conversations
keep those markers (owner ruling D5); the session chat still resolves them.
Design: knowledge-base/knowledge/features/citation_engine_overhaul.md §6.1.
"""

from __future__ import annotations

import re

#: A handle in text. The caret is optional because models drop it now and then;
#: the tools only ever print ``[^c<id>]``.
HANDLE_RE = re.compile(r"\[\^?c(\d+)\]")

#: A footnote definition line for a handle (``[^c1241]: …``), which the
#: delivery pass writes and which is not a mention.
DEFINITION_RE = re.compile(r"^\[\^?c(\d+)\]:", re.MULTILINE)

_DEFINITION_LINE_RE = re.compile(r"^\[\^?c\d+\]:.*$", re.MULTILINE)
_FENCED_CODE_RE = re.compile(
    r"^(```|~~~)[^\n]*\n.*?^\1[^\n]*$", re.MULTILINE | re.DOTALL
)
_INLINE_CODE_RE = re.compile(r"`[^`\n]*`")
_CITATION_REF_RE = re.compile(r"^\s*\[?\s*\^?\s*c?\s*(\d+)\s*\]?\s*$", re.IGNORECASE)
_SOURCE_REF_RE = re.compile(r"^\s*\[?\s*s?\s*(\d+)\s*\]?\s*$", re.IGNORECASE)


def format_handle(citation_id: int) -> str:
    """The handle an agent writes for a citation: ``[^c1241]``."""
    return f"[^c{int(citation_id)}]"


def format_source_ref(source_id: int) -> str:
    """How tools print a source id: ``S12``, never bracketed."""
    return f"S{int(source_id)}"


def parse_citation_ref(value: object) -> int:
    """Read a citation id from what an agent passed to a tool.

    Accepts the handle and the forms a model slips into: ``[^c1241]``,
    ``^c1241``, ``c1241``, ``[1241]``, ``1241`` or the integer.

    Raises:
        ValueError: ``value`` names no citation.
    """
    if isinstance(value, bool):
        raise ValueError(f"not a citation handle: {value!r}")
    if isinstance(value, int):
        return value
    match = _CITATION_REF_RE.match(str(value))
    if not match:
        raise ValueError(
            f"not a citation handle: {value!r}. Pass the handle the cite tool "
            "returned, e.g. [^c1241]."
        )
    return int(match.group(1))


def parse_source_ref(value: object) -> int:
    """Read a source id from ``S12``, ``12``, ``[12]`` or the integer.

    Raises:
        ValueError: ``value`` names no source.
    """
    if isinstance(value, bool):
        raise ValueError(f"not a source id: {value!r}")
    if isinstance(value, int):
        return value
    match = _SOURCE_REF_RE.match(str(value))
    if not match:
        raise ValueError(f"not a source id: {value!r}. Pass it as shown, e.g. S12.")
    return int(match.group(1))


def strip_code(text: str) -> str:
    """``text`` with fenced and inline code removed, where handles are literal."""
    return _INLINE_CODE_RE.sub("", _FENCED_CODE_RE.sub("", text))


def handles_in(text: str) -> list[int]:
    """Citation ids referenced in ``text``, in order of first mention.

    Code spans and blocks are skipped, and so are footnote definition lines:
    a definition is not a mention.
    """
    body = _DEFINITION_LINE_RE.sub("", strip_code(text or ""))
    seen: dict[int, None] = {}
    for match in HANDLE_RE.finditer(body):
        seen.setdefault(int(match.group(1)), None)
    return list(seen)


#: The block the delivery pass owns at the end of a document. Everything between
#: the two comments is regenerated on every delivery, so it never goes stale and
#: never doubles; HTML comments are invisible in GitHub, Obsidian and Pandoc.
DEFINITIONS_START = "<!-- srw:citations -->"
DEFINITIONS_END = "<!-- /srw:citations -->"

_DEFINITIONS_BLOCK_RE = re.compile(
    r"\n*"
    + re.escape(DEFINITIONS_START)
    + r".*?"
    + re.escape(DEFINITIONS_END)
    + r"\n*",
    re.DOTALL,
)


def replace_definitions(text: str, definitions: list[str]) -> str:
    """``text`` with its generated footnote block set to ``definitions``.

    The previous block (if any) is removed first; an empty list leaves the
    document without one. The rest of the text is untouched.
    """
    body = _DEFINITIONS_BLOCK_RE.sub("\n\n", text or "").rstrip("\n")
    if not definitions:
        return body + "\n" if body else ""
    block = "\n".join([DEFINITIONS_START, *definitions, DEFINITIONS_END])
    return f"{body}\n\n{block}\n"
