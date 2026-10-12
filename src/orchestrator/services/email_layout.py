"""Shared layout for SRW transactional email.

Email is a restricted rendering target, not a small browser. Every rule below
has a specific mechanism behind it; see
knowledge-history/done/email_and_login_theme_alignment.md before relaxing any of them.

  * Tables, not divs -- Outlook renders via the Word engine, which does not
    support `width` on <div> at all. Microsoft supports classic Outlook until
    at least 2029, so this is not a legacy concern.
  * Buttons are padded <td>s -- the Word engine has no `display` support, so an
    <a> is permanently an inline box whose vertical padding cannot expand the
    line. Square corners mean no VML is needed.
  * Colours inline -- a growing set of clients (GMX, WEB.DE, SFR, LaPoste)
    strip <head><style> entirely. The style block is enhancement only.
  * ASCII-only output -- Gmail clips messages containing non-ASCII characters
    regardless of size.
  * Uppercase via text-transform -- screen readers read the source text and
    pronounce all-caps tokens as initialisms.
"""

from __future__ import annotations

import html
from dataclasses import dataclass
from typing import Sequence

from orchestrator.services.brand import TRAVERTINE as C

# Georgia is aliased to `serif` on Android (AOSP fonts.xml aliases both Georgia
# and Times New Roman), so Gmail Android renders Noto Serif. Do not let layout
# depend on Georgia's metrics.
# Public because email_markdown.py styles body-level tags with the same stacks;
# a second copy there would drift.
SERIF = "Georgia, 'Times New Roman', serif"
SANS = "-apple-system, BlinkMacSystemFont, 'Segoe UI', Arial, sans-serif"
# Consolas ships with Office, so the Word engine resolves it without falling
# back to a proportional face the way a bare `monospace` can.
MONO = "Consolas, 'Courier New', monospace"


VARIANTS = ("primary", "approve", "deny")


@dataclass(frozen=True)
class Action:
    """A call-to-action button.

    variant:
      "primary" -- solid Tyrian (the default accent), the default CTA
      "approve" -- solid laurel
      "deny"    -- GHOST (card fill, danger border/text)

    Approve and Deny must not both be solid: #446b3e and #9c2832 measure
    1.24:1 against each other, failing WCAG 1.4.1 on the red/green axis for
    the highest-stakes action in the product. They differ by form.
    """

    label: str
    url: str
    variant: str = "primary"

    def __post_init__(self) -> None:
        # Fail loudly rather than falling through to `primary`. An unrecognised
        # variant is realistically a typo on "approve" ("Approve", "approved"),
        # and _button_cell's else-branch would then render it solid Tyrian --
        # putting a solid button next to the solid laurel Approve at 1.17:1,
        # which is the exact defect this layout exists to eliminate. A silent
        # default here reintroduces it in the one place nobody would look.
        if self.variant not in VARIANTS:
            raise ValueError(
                f"Action.variant must be one of {VARIANTS}, got {self.variant!r}"
            )


def escape_text(value: str) -> str:
    """Escape caller-supplied text destined for `body_html`."""
    return html.escape(value, quote=True)


def _to_ascii(markup: str) -> str:
    return markup.encode("ascii", "xmlcharrefreplace").decode("ascii")


def _button_cell(action: Action) -> str:
    label = html.escape(action.label, quote=True)
    url = html.escape(action.url, quote=True)
    if action.variant == "approve":
        bg, fg, border = C["success"], C["on-accent"], C["success"]
    elif action.variant == "deny":
        bg, fg, border = C["panel-bg"], C["danger"], C["danger"]
    else:
        bg, fg, border = C["accent-color"], C["on-accent"], C["accent-color"]
    return (
        f'<td align="center" bgcolor="{bg}" style="background-color:{bg};'
        f"border:2px solid {border};padding:12px 28px;"
        f"font-family:{SANS};font-size:15px;font-weight:600;"
        f'mso-line-height-rule:exactly;line-height:20px;">'
        f'<a href="{url}" style="color:{fg};text-decoration:none;'
        f"font-family:{SANS};font-size:15px;font-weight:600;"
        f'letter-spacing:1px;text-transform:uppercase;">{label}</a></td>'
    )


def render_email(
    *,
    title: str,
    body_html: str,
    subtitle: str | None = None,
    actions: Sequence[Action] = (),
    footer_note: str | None = None,
) -> str:
    """Render one branded email.

    `title`, `subtitle`, `footer_note` and every Action.label are PLAIN TEXT
    and are escaped here. `body_html` is the ONLY trusted parameter -- callers
    escape their own interpolations (use `escape_text`). This asymmetry is
    deliberate: it is what closes the injection that existed when each call
    site hand-rolled its own HTML.
    """
    safe_title = html.escape(title, quote=True)
    safe_subtitle = html.escape(subtitle, quote=True) if subtitle else ""
    safe_footer = html.escape(footer_note, quote=True) if footer_note else ""

    subtitle_row = (
        f'<p style="margin:6px 0 0 0;font-family:{SANS};font-size:13px;'
        f'line-height:18px;color:{C["text-secondary"]};">{safe_subtitle}</p>'
        if safe_subtitle
        else ""
    )

    if actions:
        # Spacer cells are interleaved between buttons; a margin on the <td>
        # would not survive the Word engine.
        button_cells = ""
        for i, action in enumerate(actions):
            if i:
                button_cells += (
                    '<td width="12" style="width:12px;font-size:0;'
                    'line-height:0;">&nbsp;</td>'
                )
            button_cells += _button_cell(action)
        actions_row = (
            f'<tr><td align="center" style="padding:8px 32px 24px 32px;">'
            f'<table role="presentation" cellpadding="0" cellspacing="0" '
            f'border="0"><tr>{button_cells}</tr></table></td></tr>'
        )
    else:
        actions_row = ""

    footer_row = (
        f'<tr><td style="padding:16px 32px 24px 32px;'
        f"border-top:1px solid {C['border-color']};font-family:{SANS};"
        f'font-size:12px;line-height:18px;color:{C["text-secondary"]};">'
        f"{safe_footer}</td></tr>"
        if safe_footer
        else ""
    )

    markup = f"""<!DOCTYPE html>
<html lang="en" dir="ltr">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="x-apple-disable-message-reformatting">
<meta name="color-scheme" content="light">
<meta name="supported-color-schemes" content="light">
<title>{safe_title}</title>
<!--[if mso]><noscript><xml><o:OfficeDocumentSettings>
<o:PixelsPerInch>96</o:PixelsPerInch></o:OfficeDocumentSettings></xml></noscript><![endif]-->
<!--[if mso]><style>td,div,p,a,h1 {{ font-family: Arial, Helvetica, sans-serif; }}</style><![endif]-->
<style>
:root {{ color-scheme: light; supported-color-schemes: light; }}
body {{ margin:0 !important; padding:0 !important; width:100% !important; }}
table {{ border-collapse:collapse; }}
img {{ border:0; outline:none; -ms-interpolation-mode:bicubic; }}
a {{ color:{C["accent-color"]}; }}
</style>
</head>
<body bgcolor="{C["app-bg"]}" style="margin:0;padding:0;background-color:{C["app-bg"]};">
<div role="article" aria-roledescription="email" lang="en" dir="ltr">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" bgcolor="{C["app-bg"]}" style="background-color:{C["app-bg"]};">
<tr><td align="center" style="padding:32px 12px;">
<table role="presentation" width="600" cellpadding="0" cellspacing="0" border="0" bgcolor="{C["panel-bg"]}" style="width:600px;max-width:600px;background-color:{C["panel-bg"]};border:1px solid {C["border-color"]};">
<tr><td bgcolor="{C["accent-color"]}" style="background-color:{C["accent-color"]};height:4px;line-height:4px;font-size:0;">&nbsp;</td></tr>
<tr><td style="padding:24px 32px 16px 32px;">
<h1 style="margin:0;font-family:{SERIF};font-size:20px;line-height:26px;font-weight:700;letter-spacing:1px;text-transform:uppercase;color:{C["text-primary"]};">{safe_title}</h1>
{subtitle_row}
</td></tr>
<tr><td style="padding:0 32px 20px 32px;font-family:{SANS};font-size:15px;line-height:24px;color:{C["text-primary"]};">
{body_html}
</td></tr>
{actions_row}
{footer_row}
</table>
</td></tr>
</table>
</div>
</body>
</html>"""
    return _to_ascii(markup)
