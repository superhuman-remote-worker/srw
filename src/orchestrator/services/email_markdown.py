"""Markdown -> email-safe HTML for the SRW transactional email body.

Agent messages and system notifications have always been *authored* in
markdown -- the parameters are named ``message_md`` / ``body_md`` -- but the
body shipped as ``escape_text(md).replace("\\n", "<br>")``, so recipients read
the literal ``**Summary:**`` and ``- item`` source. This module renders that
markdown into the same restricted HTML ``email_layout`` is built from.

It is a bounded renderer rather than a CommonMark library for two reasons:

  * Output has to survive Outlook's Word engine and the clients that strip
    ``<head><style>`` wholesale, so every emitted tag carries inline styles
    drawn from the brand palette. A general markdown library returns
    class-less semantic HTML that would then need a CSS inliner on top of it,
    and the inliner would still not know about the Word engine's rules.
  * The input is LLM-authored, i.e. untrusted. Here the text is escaped
    *first* and every tag in the output is one this module emitted, so
    ``render_email``'s contract holds unchanged: ``body_html`` is the one
    trusted parameter, and it is trusted because nothing caller-supplied
    survives into it as markup.

Supported: ATX headings, paragraphs with hard line breaks, nested bullet and
ordered lists, fenced code, blockquotes, thematic breaks, GFM pipe tables,
and inline code / bold / italic / strikethrough / links / autolinks. Anything
outside that subset degrades to its literal source text -- which is exactly
what the whole body used to do, so the failure mode is the old behaviour for
one line rather than for the message.
"""

from __future__ import annotations

import html
import re

from orchestrator.services.brand import TRAVERTINE as C
from orchestrator.services.email_layout import MONO, SANS

# Placeholders for already-rendered spans. Private-use codepoints so they
# cannot collide with anything meaningful, and stripped from the input at
# entry so a message cannot hand-write one.
_TOK_A = "\ue000"
_TOK_B = "\ue001"
_TOK_RE = re.compile(_TOK_A + r"(\d+)" + _TOK_B)

_TEXT = C["text-primary"]
_MUTED = C["text-secondary"]
_RULE = C["border-color"]
_CODE_BG = C["surface-0"]
_LINK = C["accent-color"]

# Block-level type is re-declared on every element: the wrapping <td> sets it,
# but Gmail's inline-CSS pass and the Word engine both drop inherited values in
# places, and a paragraph that falls back to Times is more obvious than any
# amount of duplication in the source.
_BODY_FONT = f"font-family:{SANS};font-size:15px;line-height:24px;"
_P_STYLE = f"margin:0 0 12px 0;{_BODY_FONT}color:{_TEXT};"
_LIST_STYLE = (
    # padding-left is what browsers honour; margin-left is what the Word engine
    # honours. Both, or the list hangs off the left edge in one of them.
    f"margin:0 0 12px 0;margin-left:6px;padding-left:22px;{_BODY_FONT}color:{_TEXT};"
)
_LI_STYLE = f"margin:0 0 6px 0;{_BODY_FONT}color:{_TEXT};"
_CODE_FONT = f"font-family:{MONO};font-size:13px;line-height:20px;"
_PRE_FONT = f"font-family:{MONO};font-size:12px;line-height:18px;"
_CELL_STYLE = f"padding:8px 10px;font-family:{SANS};font-size:14px;line-height:20px;"

# (font-size, line-height, margin) by markdown heading level. Level 1 renders
# as <h2>: the layout already owns the document's single <h1>, and a second one
# makes the message a sibling of the brand header for a screen reader.
_HEADINGS = {
    1: ("19px", "25px", "20px 0 8px 0"),
    2: ("17px", "23px", "18px 0 8px 0"),
    3: ("16px", "22px", "16px 0 6px 0"),
}
_HEADING_FALLBACK = ("15px", "21px", "14px 0 6px 0")

# Every pattern below scans a line of an untrusted, model-authored body, so each
# is written to stay linear. The tilde lookahead requires the entire delimiter
# run, so a failing info string cannot repartition tildes between both groups.
_FENCE_RE = re.compile(r"^ {0,3}(`{3,}|~{3,}(?!~))\s*(?:[^\s`]+\s*)?$")
# An ATX closing run ("## Done ##") is trimmed by _atx_text rather than by a
# "(.*?)\s*#*\s*$" tail, which offered three ways to split the same whitespace.
_HEADING_RE = re.compile(r"^ {0,3}(#{1,6})\s+(.*)")
_HR_RE = re.compile(r"^ {0,3}(?:(?:\*[ \t]*){3,}|(?:-[ \t]*){3,}|(?:_[ \t]*){3,})$")
_QUOTE_RE = re.compile(r"^ {0,3}>[ \t]?(.*)$")
# No trailing "$": "." already stops at a newline and every caller passes a
# single line, so the anchor only gave the marker's whitespace run something to
# backtrack against.
_UL_RE = re.compile(r"^( *)([-*+])[ \t]+(.*)")
_OL_RE = re.compile(r"^( *)(\d{1,9})[.)][ \t]+(.*)")
_SAFE_SCHEME_RE = re.compile(r"^(?:https?://|mailto:)", re.IGNORECASE)
# Candidate labels stop at another opener. Destination scanning also stops at
# an opener, except for a single bracketed IPv6 authority after an HTTP scheme.
# Titles are handled by _sub_links, which caches their closing delimiters.
_LINK_OPEN_RE = re.compile(r"\[([^\[\]\n]*)\]\(")
_LINK_DEST_RE = re.compile(
    r"(?:https?://(?:[^/@\s<>\[\]()]+@)?\[[^\[\]\s()<>]+\][^)\s<>\[]*"
    r"|[^)\s<>\[]+)",
    re.IGNORECASE,
)

# Nesting past this renders as flat paragraphs -- see _parse_blocks.
_MAX_DEPTH = 12
# Bare URLs are matched *after* the escape pass, so an entity has to be
# consumed whole -- "&amp;" in a query string is one character of URL, not an
# "&" that ends it.
_ENTITY = r"&(?:[a-zA-Z]+|#\d+);"
_BARE_URL_RE = re.compile(r"(?<![\w/])(https?://(?:" + _ENTITY + r"|[^\s<>\[\]()&])+)")


def render_markdown(md: str) -> str:
    """Render `md` to inline-styled HTML suitable for `render_email(body_html=...)`.

    All text is escaped; the only markup in the result is emitted here.
    """
    if not md or not md.strip():
        return ""
    text = md.replace("\r\n", "\n").replace("\r", "\n").expandtabs(4)
    text = text.replace(_TOK_A, "").replace(_TOK_B, "")
    return _blocks_to_html(_parse_blocks(text.split("\n")))


# --------------------------------------------------------------------------
# Block level
# --------------------------------------------------------------------------


def _parse_blocks(lines: list[str], depth: int = 0) -> list[tuple[str, str]]:
    """Parse into (kind, html) blocks. Kind "p" carries *inline* HTML only, so
    a list item can drop the <p> wrapper (and its margin) for a tight item.

    `depth` bounds the list/quote recursion. Nothing an agent writes nests
    _MAX_DEPTH deep, but the input is model-generated and Python's recursion
    limit is not a rendering strategy: past the bound the rest of the block
    degrades to paragraphs rather than raising inside a completion notification.
    """
    blocks: list[tuple[str, str]] = []
    i = 0
    while i < len(lines):
        line = lines[i]
        if not line.strip():
            i += 1
            continue
        if depth >= _MAX_DEPTH:
            para, i = _consume_paragraph(lines, i, plain=True)
            blocks.append(("p", _inline(para)))
        elif _FENCE_RE.match(line):
            body, i = _consume_fence(lines, i)
            blocks.append(("html", _code_block(body)))
        elif _HR_RE.match(line):  # before lists: "- - -" also matches _UL_RE
            blocks.append(("html", _thematic_break()))
            i += 1
        elif heading := _HEADING_RE.match(line):
            text = _atx_text(heading.group(2))
            blocks.append(("html", _heading(len(heading.group(1)), text)))
            i += 1
        elif _QUOTE_RE.match(line):
            inner, i = _consume_quote(lines, i)
            inner_html = _blocks_to_html(_parse_blocks(inner, depth + 1))
            blocks.append(("html", _blockquote(inner_html)))
        elif _UL_RE.match(line) or _OL_RE.match(line):
            markup, i = _consume_list(lines, i, depth)
            blocks.append(("html", markup))
        elif _is_table_start(lines, i):
            markup, i = _consume_table(lines, i)
            blocks.append(("html", markup))
        else:
            para, i = _consume_paragraph(lines, i)
            blocks.append(("p", _inline(para)))
    return blocks


def _blocks_to_html(blocks: list[tuple[str, str]]) -> str:
    return "".join(
        f'<p style="{_P_STYLE}">{body}</p>' if kind == "p" else body
        for kind, body in blocks
    )


def _starts_block(lines: list[str], i: int) -> bool:
    """Whether line `i` interrupts a running paragraph."""
    line = lines[i]
    return bool(
        _FENCE_RE.match(line)
        or _HR_RE.match(line)
        or _HEADING_RE.match(line)
        or _QUOTE_RE.match(line)
        or _UL_RE.match(line)
        or _OL_RE.match(line)
        or _is_table_start(lines, i)
    )


def _consume_paragraph(
    lines: list[str], start: int, plain: bool = False
) -> tuple[str, int]:
    """Collect one paragraph. `plain` (the depth-bound fallback) also swallows
    lines that would otherwise open a block, so the fallback cannot recurse."""
    i = start + 1
    while (
        i < len(lines) and lines[i].strip() and (plain or not _starts_block(lines, i))
    ):
        i += 1
    return "\n".join(line.strip() for line in lines[start:i]), i


def _consume_fence(lines: list[str], start: int) -> tuple[str, int]:
    fence = _FENCE_RE.match(lines[start]).group(1)  # type: ignore[union-attr]
    char, width = fence[0], len(fence)
    i = start + 1
    body: list[str] = []
    while i < len(lines):
        stripped = lines[i].strip()
        if stripped and set(stripped) == {char} and len(stripped) >= width:
            i += 1
            break
        body.append(lines[i])
        i += 1
    return "\n".join(body), i


def _consume_quote(lines: list[str], start: int) -> tuple[list[str], int]:
    inner: list[str] = []
    i = start
    while i < len(lines):
        match = _QUOTE_RE.match(lines[i])
        if match:
            inner.append(match.group(1))
        elif lines[i].strip() and inner:
            inner.append(lines[i].strip())  # lazy continuation
        else:
            break
        i += 1
    return inner, i


def _consume_list(lines: list[str], start: int, depth: int = 0) -> tuple[str, int]:
    """Collect one list. Continuation and nested lines are dedented and parsed
    recursively, so nesting depth costs nothing here."""
    ordered = _UL_RE.match(lines[start]) is None
    opener = (_OL_RE if ordered else _UL_RE).match(lines[start])
    assert opener is not None
    base = len(opener.group(1))
    first_number = int(opener.group(2)) if ordered else 1

    items: list[list[str]] = []
    i = start
    blank_pending = False
    while i < len(lines):
        line = lines[i]
        if not line.strip():
            blank_pending = True
            i += 1
            continue
        indent = len(line) - len(line.lstrip(" "))
        same_level = indent <= base and not _HR_RE.match(line)
        marker = (_OL_RE if ordered else _UL_RE).match(line) if same_level else None
        if marker:
            items.append([marker.group(3)])
            blank_pending = False
            i += 1
            continue
        if same_level and (_UL_RE.match(line) or _OL_RE.match(line)):
            break  # a list of the other kind starts here
        if indent > base and items:
            if blank_pending:
                items[-1].append("")  # loose item: keep the block separator
                blank_pending = False
            items[-1].append(line[min(indent, base + 2) :])
            i += 1
            continue
        break

    rendered = "".join(_list_item(item, depth) for item in items)
    tag = "ol" if ordered else "ul"
    start_attr = f' start="{first_number}"' if ordered and first_number != 1 else ""
    return f'<{tag}{start_attr} style="{_LIST_STYLE}">{rendered}</{tag}>', i


def _list_item(item_lines: list[str], depth: int = 0) -> str:
    blocks = _parse_blocks(item_lines, depth + 1)
    # A tight item's first paragraph goes straight into the <li>; wrapping it
    # would add a paragraph margin between the bullet and its own text.
    if blocks and blocks[0][0] == "p":
        body = blocks[0][1] + _blocks_to_html(blocks[1:])
    else:
        body = _blocks_to_html(blocks)
    return f'<li style="{_LI_STYLE}">{body}</li>'


def _is_delimiter_row(line: str) -> bool:
    stripped = line.strip()
    return bool(stripped) and set(stripped) <= set("|:- ") and "-" in stripped


def _is_table_start(lines: list[str], i: int) -> bool:
    return (
        "|" in lines[i]
        and i + 1 < len(lines)
        and "|" in lines[i + 1]
        and _is_delimiter_row(lines[i + 1])
    )


def _split_row(line: str) -> list[str]:
    stripped = line.strip()
    if stripped.startswith("|"):
        stripped = stripped[1:]
    if stripped.endswith("|") and not stripped.endswith("\\|"):
        stripped = stripped[:-1]
    return [cell.strip() for cell in re.split(r"(?<!\\)\|", stripped)]


def _consume_table(lines: list[str], start: int) -> tuple[str, int]:
    headers = _split_row(lines[start])
    aligns = []
    for spec in _split_row(lines[start + 1]):
        left, right = spec.startswith(":"), spec.endswith(":")
        aligns.append("center" if left and right else "right" if right else "left")
    aligns += ["left"] * (len(headers) - len(aligns))

    rows: list[list[str]] = []
    i = start + 2
    while i < len(lines) and lines[i].strip() and "|" in lines[i]:
        rows.append(_split_row(lines[i]))
        i += 1

    def cells(values: list[str], header: bool) -> str:
        out = ""
        for index, value in enumerate(values[: len(headers)]):
            align = aligns[index] if index < len(aligns) else "left"
            if header:
                style = (
                    f"{_CELL_STYLE}text-align:{align};font-weight:700;"
                    f"color:{_TEXT};border-bottom:1px solid {_RULE};"
                )
                out += (
                    f'<th align="{align}" scope="col" bgcolor="{_CODE_BG}" '
                    f'style="background-color:{_CODE_BG};{style}">'
                    f"{_inline(value)}</th>"
                )
            else:
                style = f"{_CELL_STYLE}text-align:{align};color:{_TEXT};border-top:1px solid {_RULE};"
                out += f'<td align="{align}" style="{style}">{_inline(value)}</td>'
        return out

    body = "".join(f"<tr>{cells(row, header=False)}</tr>" for row in rows)
    # Not role="presentation": this one carries data, and the header cells are
    # the only thing telling a screen reader what a given cell means.
    return (
        f'<table width="100%" cellpadding="0" cellspacing="0" border="0" '
        f'style="width:100%;margin:0 0 12px 0;border:1px solid {_RULE};">'
        f"<tr>{cells(headers, header=True)}</tr>{body}</table>",
        i,
    )


def _atx_text(text: str) -> str:
    """Heading text minus its optional closing run of hashes ("## Done ##").

    `_HEADING_RE` used to trim that run with a `(.*?)\\s*#*\\s*$` tail, which
    gave the engine three overlapping ways to split the same trailing
    whitespace -- quadratic on a hostile line. Two rstrips cost one pass.
    """
    text = text.rstrip()
    return text.rstrip("#").rstrip() if text.endswith("#") else text


def _heading(level: int, text: str) -> str:
    size, line_height, margin = _HEADINGS.get(level, _HEADING_FALLBACK)
    tag = f"h{min(level + 1, 6)}"
    return (
        f'<{tag} style="margin:{margin};font-family:{SANS};font-size:{size};'
        f'line-height:{line_height};font-weight:700;color:{_TEXT};">'
        f"{_inline(text)}</{tag}>"
    )


def _code_block(text: str) -> str:
    # Background on a <td>, not on the <pre>: the Word engine paints table
    # cells and ignores background on block-level content inside them.
    return (
        f'<table role="presentation" width="100%" cellpadding="0" cellspacing="0" '
        f'border="0" style="width:100%;margin:0 0 12px 0;">'
        f'<tr><td bgcolor="{_CODE_BG}" style="background-color:{_CODE_BG};'
        f'padding:12px 14px;border:1px solid {_RULE};">'
        f'<pre style="margin:0;{_PRE_FONT}color:{_TEXT};'
        f'white-space:pre-wrap;word-break:break-word;">'
        f"{html.escape(text, quote=True)}</pre></td></tr></table>"
    )


def _blockquote(inner_html: str) -> str:
    # A coloured cell rather than border-left on a <blockquote>: the Word
    # engine drops the border and the indent, leaving the quote indistinguishable
    # from the surrounding text.
    # Every block element carries its own colour, so setting one on the cell
    # would be inherited by nothing; recolour the quoted blocks instead.
    inner_html = inner_html.replace(f"color:{_TEXT};", f"color:{_MUTED};")
    return (
        f'<table role="presentation" width="100%" cellpadding="0" cellspacing="0" '
        f'border="0" style="width:100%;margin:0 0 12px 0;">'
        f'<tr><td width="3" bgcolor="{_RULE}" style="width:3px;'
        f'background-color:{_RULE};font-size:0;line-height:0;">&nbsp;</td>'
        f'<td style="padding:0 0 0 14px;{_BODY_FONT}color:{_MUTED};">'
        f"{inner_html}</td></tr></table>"
    )


def _thematic_break() -> str:
    return (
        f'<table role="presentation" width="100%" cellpadding="0" cellspacing="0" '
        f'border="0" style="width:100%;margin:16px 0;">'
        f'<tr><td height="1" style="height:1px;font-size:0;line-height:0;'
        f'border-top:1px solid {_RULE};">&nbsp;</td></tr></table>'
    )


# --------------------------------------------------------------------------
# Inline level
# --------------------------------------------------------------------------


def _inline(text: str) -> str:
    """Render inline markdown. Order matters: everything that must not be
    re-scanned (code spans, links) is replaced by a placeholder *before* the
    remaining source is escaped, so the escape pass never runs over markup we
    emitted and the emphasis pass never runs over a URL."""
    tokens: list[str] = []

    def keep(markup: str) -> str:
        tokens.append(markup)
        return f"{_TOK_A}{len(tokens) - 1}{_TOK_B}"

    text = re.sub(
        r"\\([\\`*_{}\[\]()#+\-.!>~|])",
        lambda m: keep(html.escape(m.group(1), quote=True)),
        text,
    )
    text = _sub_code_spans(text, lambda inner: keep(_code_span(inner)))
    text = re.sub(
        r"<((?:https?://|mailto:)[^\s<>]+)>",
        lambda m: keep(_anchor(m.group(1), html.escape(m.group(1), quote=True))),
        text,
    )
    text = html.escape(text, quote=True)
    text = _sub_links(
        text,
        lambda label, url: keep(_anchor(html.unescape(url), _emphasis(label))),
    )
    text = _BARE_URL_RE.sub(_bare_url(keep), text)
    text = _emphasis(text)
    return _restore(text.replace("\n", "<br>"), tokens)


def _sub_links(text: str, render) -> str:
    """Replace links without rescanning an unterminated title per opener.

    Labels and destinations scan only as far as the next candidate. Titles can
    contain brackets, so their next closing parenthesis/newline is cached as
    candidates advance. A long shared trailing whitespace run is trimmed once
    per closing parenthesis. Failed outer links still allow valid inner links.
    """
    out: list[str] = []
    copied = 0
    next_close = next_newline = -2  # -2: not searched; -1: no remaining delimiter
    title_end = -1
    length = len(text)
    for opening in _LINK_OPEN_RE.finditer(text):
        if opening.start() < copied:
            continue
        pos = opening.end()
        while pos < length and text[pos] in " \t":
            pos += 1
        if pos < length and text[pos] == "<":
            pos += 1
        destination = _LINK_DEST_RE.match(text, pos)
        if destination is None:
            continue
        pos = destination.end()
        if pos < length and text[pos] == ">":
            pos += 1
        separator = pos
        while pos < length and text[pos] in " \t":
            pos += 1
        if pos < length and text[pos] == ")":
            end = pos + 1
        elif pos > separator and text.startswith("&quot;", pos):
            content = pos + len("&quot;")
            if next_close == -2 or (next_close >= 0 and next_close < content):
                next_close = text.find(")", content)
                title_end = next_close
                while title_end > content and text[title_end - 1] in " \t":
                    title_end -= 1
            if next_newline == -2 or (next_newline >= 0 and next_newline < content):
                next_newline = text.find("\n", content)
            if (
                next_close < 0
                or (next_newline >= 0 and next_newline < next_close)
                or title_end - len("&quot;") < content
                or not text.startswith("&quot;", title_end - len("&quot;"))
            ):
                continue
            end = next_close + 1
        else:
            continue
        out.append(text[copied : opening.start()])
        out.append(render(opening.group(1), destination.group()))
        copied = end
    out.append(text[copied:])
    return "".join(out)


def _sub_code_spans(text: str, render) -> str:
    """Match backtick runs to the next run of exactly the same width.

    Index runs once, then map each one to its next same-width partner in a
    reverse pass. Unmatched widths never rescan the remaining runs or text.
    Both passes and the emitted slices cost O(len(text)) total.
    """
    runs = [(match.start(), match.end()) for match in re.finditer(r"`+", text)]
    following: list[int | None] = [None] * len(runs)
    by_width: dict[int, int] = {}
    for index in range(len(runs) - 1, -1, -1):
        start, end = runs[index]
        width = end - start
        following[index] = by_width.get(width)
        by_width[width] = index
    out: list[str] = []
    copied = index = 0
    while index < len(runs):
        partner = following[index]
        if partner is None:
            index += 1
            continue
        start, end = runs[index]
        close_start, close_end = runs[partner]
        out.append(text[copied:start])
        out.append(render(text[end:close_start]))
        copied = close_end
        index = partner + 1
    out.append(text[copied:])
    return "".join(out)


def _ends_with_entity(text: str, end: int) -> bool:
    """Whether `text[:end]` ends in an unfinished entity: `&name` or `&#digits`.

    Scans backwards over the entity body only, so it costs O(len(entity)) rather
    than a search from the start of the string."""
    start = end
    while start and text[start - 1].isascii() and text[start - 1].isalpha():
        start -= 1
    if start < end:
        return start > 0 and text[start - 1] == "&"
    while start and text[start - 1].isdecimal():
        start -= 1
    return (
        start < end and start > 1 and text[start - 1] == "#" and text[start - 2] == "&"
    )


def _bare_url(keep):
    def replace(match: re.Match[str]) -> str:
        url = match.group(1)
        # Sentence punctuation belongs to the prose, not the URL -- except when
        # the ";" is the tail of an entity the escape pass wrote (a link with a
        # quote in it would otherwise be cut in half and displayed as "&quot").
        end = len(url)
        while end and url[end - 1] in ".,;:!?" and not _ends_with_entity(url, end - 1):
            end -= 1
        if not end:
            return match.group(0)
        trailing = url[end:]
        url = url[:end]
        return keep(_anchor(html.unescape(url), url)) + trailing

    return replace


def _code_span(inner: str) -> str:
    inner = inner.replace("\n", " ")
    if len(inner) > 2 and inner.startswith(" ") and inner.endswith(" "):
        inner = inner[1:-1]
    return (
        f'<code style="background-color:{_CODE_BG};padding:1px 5px;'
        f'{_CODE_FONT}color:{_TEXT};">{html.escape(inner, quote=True)}</code>'
    )


def _anchor(url: str, label_html: str) -> str:
    """Anchor for `url`, or the bare label if the scheme is not one we allow.

    Dropping the anchor rather than the whole span is deliberate: a `javascript:`
    link in an agent message is far more likely to be a quoted snippet than an
    attack, and the recipient should still see what it said."""
    url = url.strip()
    if not _SAFE_SCHEME_RE.match(url):
        return label_html
    href = html.escape(url, quote=True)
    return (
        f'<a href="{href}" style="color:{_LINK};text-decoration:underline;">'
        f"{label_html}</a>"
    )


def _emphasis(text: str) -> str:
    """Apply bold / italic / strikethrough to already-escaped text.

    The `_` variants require a non-word character on both sides, or every
    `snake_case_identifier` in an agent message turns into italics."""
    # "(.+?)" spanning a "**" of its own is what made these quadratic: every
    # opener lazily rescanned the rest of the line, and a closer rejected by the
    # lookarounds sent it back to the next opener. A span that cannot contain
    # its own delimiter gives each character one chance to be consumed.
    text = re.sub(
        r"(?<!\*)\*\*(?!\s)((?:[^*\n]|\*(?!\*))+)(?<!\s)\*\*(?!\*)",
        r"<strong>\1</strong>",
        text,
    )
    text = re.sub(
        r"(?<![\w_])__(?!\s)((?:[^_\n]|_(?!_))+)(?<!\s)__(?!\w)",
        r"<strong>\1</strong>",
        text,
    )
    text = re.sub(r"(?<![\*\w])\*(?!\s)([^*\n]+?)(?<!\s)\*(?!\*)", r"<em>\1</em>", text)
    text = re.sub(r"(?<![\w_])_(?!\s)([^_\n]+?)(?<!\s)_(?!\w)", r"<em>\1</em>", text)
    # Not flagged by CodeQL, but the same "(.+?)" shape as the two above and
    # measurably quadratic on a run of unclosed "~~" openers.
    return re.sub(r"~~(?!\s)((?:[^~\n]|~(?!~))+)(?<!\s)~~", r"<s>\1</s>", text)


def _restore(text: str, tokens: list[str]) -> str:
    """Splice rendered spans back in. Iterative because a link's label can
    itself hold a placeholder (a code span inside the link text)."""

    def swap(match: re.Match[str]) -> str:
        index = int(match.group(1))
        return tokens[index] if index < len(tokens) else ""

    for _ in range(6):
        if _TOK_A not in text:
            break
        text = _TOK_RE.sub(swap, text)
    # Any sentinel still standing would reach the client as a literal
    # private-use character (or, after _to_ascii, as "&#57344;").
    return text.replace(_TOK_A, "").replace(_TOK_B, "")
