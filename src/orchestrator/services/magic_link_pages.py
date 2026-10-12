"""HTML pages for emailed permission magic links (R1.B10).

Email magic-links land at ``/magic/approve/{token}``. GET renders a
confirmation page (read-only, prefetch-safe); its buttons POST the decision or
an attention-window extension. Every dynamic value is HTML-escaped at its sink.
These renderers are pure: no database, network or application state.
"""

from __future__ import annotations

import html
import os
import urllib.parse
from typing import Optional

from orchestrator.services.brand import TRAVERTINE as _BRAND

# Phase 5: per-thread cap on /magic/extend clicks. 4 × 60min = 4h total
# awaiting_user before unconditional suspension. Configurable via env for
# ops tuning during incident response.
MAGIC_EXTEND_CAP: int = int(os.environ.get("HEADLESS_EXTEND_CAP", "4"))


def magic_link_confirmation_page(
    *,
    tool_name: str,
    tool_args_preview: str,
    intended_decision: Optional[str],
    token: str,
    extend_status: Optional[str] = None,
    extends_remaining: Optional[int] = None,
) -> str:
    """Render the GET landing page. Single button POSTs back to the same
    URL with the actual decision; this is what prevents email-link
    prefetchers (Outlook Safe Links, Gmail) from auto-consuming tokens.

    Phase 5: a second form lets the user POST /magic/extend/{token} to
    bump the attention-sleep clock by 60 min without consuming the
    approval token. extend_status (when set) drives an inline toast:
    'extended' on success, 'cap_reached' when extend_count >= cap,
    'not_awaiting' when the thread is no longer in awaiting_user.
    """
    # Both values come from the agent's pending tool call and land in element
    # content; the token below lands in an attribute. html.escape(quote=True)
    # covers & < > " ' in one pass — the hand-rolled chains here missed ">" on
    # the tool name and the quotes on both, which is the reflected-XSS hole.
    safe_args = html.escape(tool_args_preview, quote=True)
    safe_tool = html.escape(tool_name, quote=True)
    if intended_decision == "approved":
        button_label = "Confirm: Approve"
        button_color = _BRAND["success"]
    elif intended_decision == "denied":
        button_label = "Confirm: Deny"
        button_color = _BRAND["danger"]
    else:
        button_label = "Confirm decision"
        button_color = _BRAND["accent-color"]

    # The token lands in a form ``action`` attribute. Percent-encoding already
    # removes every character that could close the attribute; escaping the
    # result as well is a no-op on that output but keeps the sanitizer
    # explicit at the sink rather than inferred from the encoder.
    quoted_token = html.escape(urllib.parse.quote(token, safe=""), quote=True)

    # Extend banner copy — friendly, action-specific.
    extend_banner_html = ""
    if extend_status == "extended":
        remaining_str = (
            f" — {extends_remaining} extends remaining"
            if extends_remaining is not None
            else ""
        )
        extend_banner_html = (
            f'<div style="background: {_BRAND["surface-0"]}; border: 1px solid {_BRAND["success"]}; '
            "padding: 10px 12px; margin: 0 0 12px 0; "
            f'color: {_BRAND["success"]}; font-size: 13px;">Window extended by 60 minutes'
            f"{remaining_str}.</div>"
        )
    elif extend_status == "cap_reached":
        extend_banner_html = (
            f'<div style="background: {_BRAND["surface-0"]}; border: 1px solid {_BRAND["text-secondary"]}; '
            "padding: 10px 12px; margin: 0 0 12px 0; "
            f'color: {_BRAND["text-secondary"]}; font-size: 13px;">Extend limit reached — please '
            "approve, deny, or open the cockpit.</div>"
        )
    elif extend_status == "not_awaiting":
        extend_banner_html = (
            f'<div style="background: {_BRAND["surface-0"]}; border: 1px solid {_BRAND["accent-color"]}; '
            "padding: 10px 12px; margin: 0 0 12px 0; "
            f'color: {_BRAND["accent-color"]}; font-size: 13px;">No extend needed — the agent '
            "is already active.</div>"
        )

    # Disable the extend button if we already know the cap was hit.
    #
    # The disabled look MUST be merged into the button's own style attribute.
    # HTML keeps the FIRST style= on an element and ignores every later one,
    # so emitting a second one meant the cap_reached branch -- and only that
    # branch -- rendered a button with opacity/cursor and none of the brand
    # colours, border or type scale.
    _extend_cap_reached = extend_status == "cap_reached"
    extend_disabled_attr = " disabled" if _extend_cap_reached else ""
    extend_button_style = (
        f"background: transparent; color: {_BRAND['accent-color']}; "
        f"padding: 10px 20px; border: 1px solid {_BRAND['accent-color']}; "
        f"font-weight: 600; font-size: 14px; "
        + (
            "opacity: 0.5; cursor: not-allowed;"
            if _extend_cap_reached
            else "cursor: pointer;"
        )
    )

    return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>SRW — Confirm Decision</title></head>
<body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: {_BRAND["app-bg"]}; color: {_BRAND["text-primary"]}; padding: 40px 20px;">
  <div style="max-width: 600px; margin: 0 auto; background: {_BRAND["panel-bg"]}; border: 1px solid {_BRAND["border-color"]}; overflow: hidden;">
    <div style="background: {_BRAND["surface-0"]}; padding: 16px 20px; border-bottom: 1px solid {_BRAND["border-color"]};">
      <h2 style="margin: 0; color: {_BRAND["accent-color"]}; font-size: 16px;">Confirm tool decision</h2>
    </div>
    <div style="padding: 20px; font-size: 14px; line-height: 1.6;">
      {extend_banner_html}
      <p>The agent wants to call <code style="background: {_BRAND["surface-0"]}; padding: 2px 6px;">{safe_tool}</code> with these arguments:</p>
      <pre style="background: {_BRAND["surface-0"]}; padding: 12px; overflow-x: auto; font-size: 12px; color: {_BRAND["success"]};">{safe_args}</pre>
    </div>
    <div style="background: {_BRAND["surface-0"]}; padding: 16px 20px; border-top: 1px solid {_BRAND["border-color"]}; text-align: center;">
      <form method="POST" action="/magic/approve/{quoted_token}" style="display: inline;">
        <button type="submit" style="background: {button_color}; color: {_BRAND["on-accent"]}; padding: 10px 28px; border: 0; cursor: pointer; font-weight: 600; font-size: 14px;">{button_label}</button>
      </form>
      <form method="POST" action="/magic/extend/{quoted_token}" style="display: inline; margin-left: 8px;">
        <button type="submit"{extend_disabled_attr} style="{extend_button_style}">I'm reviewing — extend 60min</button>
      </form>
      <p style="margin: 16px 0 0 0; color: {_BRAND["text-secondary"]}; font-size: 12px;">Approve link is single-use and expires in 30 minutes.</p>
    </div>
  </div>
</body></html>"""


def magic_link_result_page(
    *,
    title: str,
    body: str,
    cockpit_url: str,
    is_error: bool = False,
) -> str:
    accent = _BRAND["danger"] if is_error else _BRAND["success"]
    return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>SRW — {title}</title></head>
<body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: {_BRAND["app-bg"]}; color: {_BRAND["text-primary"]}; padding: 40px 20px;">
  <div style="max-width: 600px; margin: 0 auto; background: {_BRAND["panel-bg"]}; border: 1px solid {_BRAND["border-color"]}; overflow: hidden;">
    <div style="background: {_BRAND["surface-0"]}; padding: 16px 20px; border-bottom: 1px solid {_BRAND["border-color"]};">
      <h2 style="margin: 0; color: {accent}; font-size: 16px;">{title}</h2>
    </div>
    <div style="padding: 20px; font-size: 14px; line-height: 1.6;">
      <p>{body}</p>
      <p style="margin-top: 16px;"><a href="{cockpit_url}" style="color: {_BRAND["accent-color"]};">Open the cockpit</a></p>
    </div>
  </div>
</body></html>"""


__all__ = [
    "MAGIC_EXTEND_CAP",
    "magic_link_confirmation_page",
    "magic_link_result_page",
]
