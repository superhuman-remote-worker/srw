"""The email body renders markdown, and renders it as *our* markup only.

Two properties are load-bearing here and each assertion below belongs to one
of them:

  1. The markdown an agent writes reaches the recipient as formatting, not as
     literal `**Summary:**` source (the defect this module exists to fix).
  2. `render_email(body_html=...)` stays trustworthy. The body is the one
     parameter the layout does not escape, so a message that can smuggle a tag
     through here is an injection into every SRW email.
"""

import re

import pytest

from orchestrator.services import brand
from orchestrator.services.email_markdown import _bare_url, render_markdown


class TestFormatting:
    def test_emphasis_and_code_become_tags(self) -> None:
        html = render_markdown("**Summary:** ran `pytest` *twice*")
        assert "<strong>Summary:</strong>" in html
        assert "<em>twice</em>" in html
        assert "<code" in html and ">pytest</code>" in html
        assert "**" not in html and "`" not in html

    def test_bullet_list_becomes_a_list(self) -> None:
        html = render_markdown("Deliverables:\n\n- `a.md`\n- b.md")
        assert html.count("<li") == 2
        assert "<ul" in html
        assert "- b.md" not in html

    def test_ordered_list_keeps_its_first_number(self) -> None:
        html = render_markdown("3. third\n4. fourth")
        assert "<ol" in html and 'start="3"' in html
        assert html.count("<li") == 2

    def test_nested_list_nests(self) -> None:
        html = render_markdown("- outer\n  - inner")
        assert re.search(r"<li[^>]*>outer<ul", html)

    def test_headings_start_below_the_layout_h1(self) -> None:
        # The layout owns the single <h1>; a second one would make the message
        # a sibling of the brand header for a screen reader.
        html = render_markdown("# Top\n\n## Next")
        assert "<h2" in html and "<h3" in html
        assert "<h1" not in html

    def test_fenced_code_survives_verbatim(self) -> None:
        html = render_markdown('```python\nprint("a < b")\n```')
        assert "<pre" in html
        assert "print(&quot;a &lt; b&quot;)" in html

    def test_pipe_table_becomes_a_table_with_header_cells(self) -> None:
        html = render_markdown("| Check | Result |\n| --- | ---: |\n| ruff | clean |")
        assert '<th align="left" scope="col"' in html
        assert '<td align="right"' in html  # ---: is a right-aligned column
        assert "| Check |" not in html

    def test_data_table_is_not_presentational(self) -> None:
        # role="presentation" on a table with <th>s tells a screen reader to
        # ignore the very headers that make the cells mean anything.
        html = render_markdown("| a | b |\n| --- | --- |\n| 1 | 2 |")
        table = re.search(r"<table[^>]*>(?=(?:(?!<table).)*<th)", html, re.S)
        assert table and 'role="presentation"' not in table.group(0)

    def test_blockquote_and_thematic_break(self) -> None:
        html = render_markdown("> quoted\n\n---\n\ntail")
        assert "quoted" in html and "&gt; quoted" not in html
        assert "border-top:1px solid" in html

    def test_single_newlines_stay_line_breaks(self) -> None:
        # Agent messages lean on soft breaks; CommonMark would collapse them.
        assert "<br>" in render_markdown("line one\nline two")

    def test_plain_text_still_renders(self) -> None:
        html = render_markdown("nothing special here")
        assert "<p" in html and "nothing special here" in html

    def test_empty_input_renders_nothing(self) -> None:
        assert render_markdown("") == ""
        assert render_markdown("   \n\n ") == ""


class TestIdentifiersAreNotFormatting:
    """Agent messages are full of paths, flags and identifiers. Treating those
    as emphasis mangles the one thing the reader needs to copy exactly."""

    def test_underscores_inside_a_word_are_literal(self) -> None:
        html = render_markdown("call snake_case_name in job_frozen_data")
        assert "<em>" not in html
        assert "snake_case_name" in html

    def test_backslash_escapes_the_marker(self) -> None:
        html = render_markdown(r"literal \*asterisks\* here")
        assert "<em>" not in html
        assert "*asterisks*" in html

    def test_arithmetic_is_not_emphasis(self) -> None:
        assert "<em>" not in render_markdown("2 * 3 * 4")


class TestPathologicalInput:
    """The body is model-generated, so "no agent would write that" is not a
    bound. Failing here raises inside a job-completion notification."""

    @pytest.mark.parametrize(
        "md",
        [
            "\n".join("  " * i + "- x" for i in range(400)),  # nested lists
            "\n".join(">" * i + " q" for i in range(1, 400)),  # nested quotes
        ],
    )
    def test_deep_nesting_flattens_instead_of_raising(self, md: str) -> None:
        html = render_markdown(md)
        assert html
        assert html.count("<ul") <= 12  # _MAX_DEPTH, then paragraphs

    @pytest.mark.parametrize(
        "md", ["**" * 4000, "`" * 5000, "| a |\n| --- |\n" + "| 1 |\n" * 2000]
    )
    def test_unbalanced_markers_do_not_blow_up(self, md: str) -> None:
        render_markdown(md)  # completes; the assertion is that it returns


class TestUntrustedInput:
    def test_html_in_the_message_is_escaped(self) -> None:
        html = render_markdown("<script>alert(1)</script>\n\n<img src=x onerror=go>")
        assert "<script>" not in html and "<img" not in html
        assert "&lt;script&gt;" in html

    def test_html_inside_code_is_escaped(self) -> None:
        assert "<b>" not in render_markdown("`<b>` and ```\n<b>\n```")

    @pytest.mark.parametrize(
        "url",
        [
            "javascript:alert(1)",
            "JaVaScRiPt:alert(1)",
            "data:text/html,<script>alert(1)</script>",
            "file:///etc/passwd",
        ],
    )
    def test_unsafe_link_schemes_lose_the_anchor_not_the_text(self, url: str) -> None:
        html = render_markdown(f"see [here]({url})")
        assert "<a href" not in html
        assert "here" in html  # the reader still gets to see what it said

    def test_safe_link_keeps_its_query_string_escaped(self) -> None:
        html = render_markdown("[x](https://c.example/i?job=1&thread=2)")
        assert 'href="https://c.example/i?job=1&amp;thread=2"' in html

    def test_quotes_in_a_url_cannot_close_the_attribute(self) -> None:
        html = render_markdown('[x](https://e.example/" onmouseover="go)')
        assert 'href="https://e.example/&quot;"' in html
        assert '" onmouseover="' not in html  # never reopens as an attribute

    def test_entities_are_not_split_by_the_bare_url_scan(self) -> None:
        # The scan runs on escaped text; matching half of "&amp;" would emit a
        # link whose visible text ends in a broken entity.
        html = render_markdown("see https://e.example/a?b=1&c=2 now")
        assert 'href="https://e.example/a?b=1&amp;c=2"' in html
        assert ">https://e.example/a?b=1&amp;c=2</a>" in html

    def test_bare_urls_are_linked_without_trailing_punctuation(self) -> None:
        html = render_markdown("Full diff: https://git.example/a/b. Thanks")
        assert 'href="https://git.example/a/b"' in html
        assert ">https://git.example/a/b</a>." in html

    @pytest.mark.parametrize(
        ("text", "href", "after"),
        [
            ("see https://e.example/a. Next", "https://e.example/a", "."),
            ("(see https://e.example/a), ok", "https://e.example/a", "),"),
            ("really https://e.example/a?!", "https://e.example/a", "?!"),
            ("https://e.example/a...,;:", "https://e.example/a", "...,;:"),
        ],
    )
    def test_bare_url_trailing_punctuation_stays_out_of_the_link(
        self, text: str, href: str, after: str
    ) -> None:
        html = render_markdown(text)
        assert f'href="{href}"' in html
        assert f">{href}</a>{after}" in html.replace("&#x27;", "'")

    def test_bare_url_keeps_an_entity_semicolon(self) -> None:
        # The escape pass writes the quote as "&quot;"; its ";" is not prose.
        html = render_markdown('see https://e.example/a". Next')
        assert 'href="https://e.example/a&quot;"' in html
        assert "</a>. Next" in html

    def test_bare_url_made_only_of_punctuation_falls_through_unchanged(self) -> None:
        # The URL pattern always matches a scheme, so reach the guard directly.
        match = re.match(r"(.*)", ".,;:!?")
        assert _bare_url(lambda anchor: "LINK")(match) == ".,;:!?"

    def test_placeholder_sentinels_cannot_be_smuggled_in(self) -> None:
        # The renderer swaps rendered spans out for private-use sentinels; a
        # message that writes one itself must not be able to address a span.
        html = render_markdown("\ue0000\ue001 and `real code`")
        assert "\ue000" not in html and "\ue001" not in html
        assert ">real code</code>" in html


class TestStaysWithinTheEmailContract:
    def test_uses_no_unmanaged_colours(self) -> None:
        palette = {brand.normalize_hex(v) for v in brand.TRAVERTINE.values()}
        html = render_markdown(
            "# h\n\ntext `code` [l](https://e.example)\n\n> q\n\n"
            "- a\n\n```\nx\n```\n\n| a |\n| --- |\n| 1 |\n\n---\n"
        )
        used = {brand.normalize_hex(m) for m in re.findall(r"#[0-9a-fA-F]{3,6}", html)}
        assert used <= palette, f"unmanaged colours: {sorted(used - palette)}"

    def test_every_styled_tag_carries_its_style_inline(self) -> None:
        # Clients that strip <head><style> are the reason the layout inlines
        # everything; a body tag with a class instead would render unstyled.
        html = render_markdown("# h\n\n- a\n\n> q\n\n`c`\n\n```\nx\n```")
        assert "class=" not in html
        for tag in ("<p ", "<h2 ", "<ul ", "<li ", "<code ", "<pre "):
            assert tag in html, f"{tag!r} missing — style would have to be global"

    def test_survives_the_layouts_ascii_pass(self) -> None:
        from orchestrator.services.email_layout import render_email

        html = render_email(
            title="t", body_html=render_markdown("em dash — and `café`")
        )
        assert html.isascii()
        assert "&#8212;" in html and "caf&#233;" in html


class TestServiceBodies:
    def test_agent_message_body_is_rendered_markdown(self) -> None:
        from orchestrator.services.email import EmailService

        html = EmailService()._build_agent_message_html(
            message_md="**Done.**\n\n- `a.md`",
            job_description="job",
            config_name="developer",
            phase_str="phase 1",
            cockpit_link="https://cockpit.test/",
            reply_to_addr=None,
        )
        assert "<strong>Done.</strong>" in html
        assert "**Done.**" not in html
        assert "<li" in html

    def test_system_notification_body_is_rendered_markdown(self) -> None:
        from orchestrator.services.email import EmailService

        html = EmailService()._build_system_notification_html(
            to_name="Ada",
            body_md="Automation **paused** after 3 failures.",
            cockpit_link="https://cockpit.test/",
        )
        assert "Hello Ada," in html
        assert "<strong>paused</strong>" in html
