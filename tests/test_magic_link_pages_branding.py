"""The magic-link landing pages share the email's palette.

They are the click target of the permission email's Approve/Deny buttons, so
they must not be a design generation behind it.

_pages() must actually exercise every branch that carries its own literal
colours -- intended_decision's three arms (approved / denied / neutral) and
extend_status's three arms (extended / cap_reached / not_awaiting) -- or a
Catppuccin hex sitting in an untriggered branch passes silently. (This bit
once: the original fixture passed intended_decision="approve", which matches
neither the "approved" nor "denied" branch, so every render fell through to
the neutral else and two branches' hexes were never checked.)
"""

import re

from orchestrator.services import brand
from orchestrator.services.magic_link_pages import (
    magic_link_confirmation_page,
    magic_link_result_page,
)

CATPPUCCIN = {
    "#1e1e2e",
    "#181825",
    "#313244",
    "#cdd6f4",
    "#cba6f7",
    "#a6e3a1",
    "#f38ba8",
    "#6c7086",
    "#a6adc8",
    "#89b4fa",
    "#f9e2af",
}


def _pages() -> list[str]:
    return [
        # intended_decision arms -- each picks its own button_color literal.
        magic_link_confirmation_page(
            tool_name="run_command",
            tool_args_preview='{"cmd": "ls"}',
            intended_decision="approved",
            token="T1",
        ),
        magic_link_confirmation_page(
            tool_name="run_command",
            tool_args_preview='{"cmd": "ls"}',
            intended_decision="denied",
            token="T1",
        ),
        magic_link_confirmation_page(
            tool_name="run_command",
            tool_args_preview='{"cmd": "ls"}',
            intended_decision=None,
            token="T1",
        ),
        # extend_status arms -- each renders its own inline banner literal.
        magic_link_confirmation_page(
            tool_name="run_command",
            tool_args_preview='{"cmd": "ls"}',
            intended_decision=None,
            token="T1",
            extend_status="extended",
            extends_remaining=2,
        ),
        magic_link_confirmation_page(
            tool_name="run_command",
            tool_args_preview='{"cmd": "ls"}',
            intended_decision="denied",
            token="T1",
            extend_status="cap_reached",
        ),
        magic_link_confirmation_page(
            tool_name="run_command",
            tool_args_preview='{"cmd": "ls"}',
            intended_decision=None,
            token="T1",
            extend_status="not_awaiting",
        ),
        magic_link_result_page(
            title="Approved",
            body="The agent may proceed.",
            cockpit_url="https://cockpit.test/",
        ),
        # is_error=True picks a different accent -- cover both branches.
        magic_link_result_page(
            title="Link expired",
            body="This approval link is no longer valid.",
            cockpit_url="https://cockpit.test/",
            is_error=True,
        ),
    ]


def test_no_catppuccin_remains() -> None:
    for page in _pages():
        found = {h.lower() for h in re.findall(r"#[0-9a-fA-F]{6}", page)}
        assert not (found & CATPPUCCIN), f"Catppuccin remains: {found & CATPPUCCIN}"


def test_pages_use_the_brand_palette() -> None:
    for page in _pages():
        assert brand.TRAVERTINE["panel-bg"] in page
        assert brand.TRAVERTINE["text-primary"] in page


def test_no_text_muted_anywhere() -> None:
    """text-muted fails WCAG AA on every Travertine surface (see
    tests/test_brand_palette.py). Both pages are short enough that a
    document-wide absence check is meaningful -- footer/legal text on these
    pages must use text-secondary instead, so its hex should never appear.
    """
    for page in _pages():
        assert brand.TRAVERTINE["text-muted"] not in page


def test_disabled_extend_button_keeps_its_brand_styling() -> None:
    """One style attribute, not two.

    HTML keeps the FIRST style= on an element and ignores every later one, so
    appending a second for the disabled look meant the cap_reached branch --
    and only that branch -- rendered a button with opacity/cursor and none of
    the brand colour, border or type scale. The palette tests above cannot see
    it: the page as a whole still contains every brand hex.
    """
    page = magic_link_confirmation_page(
        tool_name="run_command",
        tool_args_preview='{"cmd": "ls"}',
        intended_decision="denied",
        token="T1",
        extend_status="cap_reached",
    )
    form = re.search(
        r'<form[^>]*action="/magic/extend/[^"]*"[^>]*>.*?</form>', page, re.S
    )
    assert form, "no extend form on the confirmation page"
    button = re.search(r"<button[^>]*>", form.group(0))
    assert button, "no button inside the extend form"
    markup = button.group(0)

    assert " disabled" in markup, "the cap_reached button must be disabled"
    assert markup.count("style=") == 1, (
        f"{markup.count('style=')} style attributes -- the browser honours only "
        "the first, so the later one is dead:\n" + markup
    )
    assert "not-allowed" in markup, "the disabled affordance was dropped"
    assert brand.TRAVERTINE["accent-color"] in markup, (
        "the disabled button lost its brand colour"
    )
