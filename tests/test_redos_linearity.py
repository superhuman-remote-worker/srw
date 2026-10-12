"""The regexes reached by untrusted text run in linear time.

CodeQL's ``py/polynomial-redos`` flagged 26 sites where a pattern applied to
user-provided text backtracks polynomially: an email body, a knowledge note, a
repository URL or a line of git output can be shaped so one ``re`` call spends
seconds of CPU. On a multi-tenant service that is a denial-of-service vector,
so each site below carries two kinds of test:

  * **Characterization** — the behaviour on ordinary input, pinned before the
    rewrite so a faster pattern cannot quietly become a different one.
  * **Adversarial** — the hostile shape that made the *original* pattern
    quadratic, at ``HOSTILE`` characters, asserted to finish inside ``BUDGET``.

The adversarial bound is wall-clock, which is not perfectly reproducible, but
the margin is what makes it meaningful: every original pattern here needed tens
of seconds (two ran past 60s) on the same input this file completes in
milliseconds. A regression would miss the bound by orders of magnitude, not by
a few percent.
"""

import time

import pytest

from agent.managers.git_manager import GitManager
from orchestrator.services import email_markdown as em
from orchestrator.services import kb_reindex, tts
from shared.runtime.core.managed_repository import repository_url_has_credentials
from shared.runtime.knowledge import chunker, gardener

# The brief's floor: a hostile input of at least 100 000 characters.
HOSTILE = 100_000
# "Well under a second" — the original patterns needed 30-60+ seconds here.
BUDGET = 1.0


def assert_fast(fn, arg, budget: float = BUDGET):
    """Run `fn(arg)` and fail if it took longer than `budget` seconds.

    The argument is built by the caller, so construction cost stays out of the
    measurement and the number is the scan itself.
    """
    start = time.perf_counter()
    result = fn(arg)
    elapsed = time.perf_counter() - start
    assert elapsed < budget, f"{fn} took {elapsed:.3f}s on {len(arg)} chars"
    return result


class TestEmailMarkdownBlocks:
    """Block scanners in the email renderer (alerts #466-#479).

    Every one of these runs per line of an agent-authored message, and
    ``_starts_block`` runs all of them on the same line again.
    """

    def test_fence_openers_are_recognized(self) -> None:
        assert em._FENCE_RE.match("```python").group(1) == "```"
        assert em._FENCE_RE.match("   ~~~~").group(1) == "~~~~"
        # Four spaces is an indented code block, not a fence.
        assert em._FENCE_RE.match("    ```") is None

    def test_headings_keep_their_level_and_text(self) -> None:
        assert em._HEADING_RE.match("## Status").groups() == ("##", "Status")
        assert em._HEADING_RE.match("#Nospace") is None
        # A closing run of hashes is decoration, not part of the text. The trim
        # moved out of the pattern's tail into _atx_text; the text is the same.
        heading = em._HEADING_RE.match("### Done ###")
        assert heading.group(1) == "###"
        assert em._atx_text(heading.group(2)) == "Done"
        assert em._atx_text("A ### B ###") == "A ### B"
        assert em._atx_text("plain") == "plain"

    def test_list_markers_keep_indent_and_content(self) -> None:
        assert em._UL_RE.match("- item").groups() == ("", "-", "item")
        assert em._UL_RE.match("  * nested").groups() == ("  ", "*", "nested")
        assert em._UL_RE.match("-nospace") is None
        assert em._OL_RE.match("3. third").groups() == ("", "3", "third")
        assert em._OL_RE.match("  10) ten").groups() == ("  ", "10", "ten")

    def test_a_realistic_message_still_renders(self) -> None:
        html = em.render_markdown(
            "# Job complete\n\n"
            "Ran `pytest` **twice**:\n\n"
            "- `tests/test_a.py` — 12 passed\n"
            "- `tests/test_b.py` — 3 passed\n\n"
            "> No regressions.\n\n"
            "See https://example.invalid/runs/1 for the log.\n"
        )
        assert "<h2" in html and html.count("<li") == 2
        assert "<strong>twice</strong>" in html
        assert "<blockquote" in html or "<table" in html

    def test_fence_info_string_run_is_linear(self) -> None:
        # "\s*[^\s`]*\s*$": two whitespace runs around an optional info word.
        assert_fast(em._FENCE_RE.match, "```" + " " * HOSTILE + "`")

    def test_heading_trailing_hash_run_is_linear(self) -> None:
        # "(.*?)\s*#*\s*$": three overlapping ways to split one tail.
        half = HOSTILE // 2
        assert_fast(em._HEADING_RE.match, "# h" + " " * half + "#" + " " * half + "x")

    def test_starts_block_runs_every_scanner_linearly(self) -> None:
        half = HOSTILE // 2
        line = "# h" + " " * half + "#" + " " * half + "x"
        assert_fast(lambda s: em._starts_block([s], 0), line)

    def test_unordered_marker_whitespace_run_is_linear(self) -> None:
        # "[ \t]+(.*)$" overlap; the newline makes the anchor fail.
        assert_fast(em._UL_RE.match, "- " + " " * HOSTILE + "\nX")

    def test_ordered_marker_whitespace_run_is_linear(self) -> None:
        assert_fast(em._OL_RE.match, "1. " + " " * HOSTILE + "\nX")


class TestEmailMarkdownInline:
    """Inline rendering of an untrusted message body (alerts #480-#483)."""

    def test_code_spans_and_links_render(self) -> None:
        html = em._inline("call `run()` in [the docs](https://example.invalid/d)")
        assert ">run()</code>" in html
        assert 'href="https://example.invalid/d"' in html
        assert ">the docs</a>" in html

    def test_emphasis_renders(self) -> None:
        assert em._emphasis("**bold**") == "<strong>bold</strong>"
        assert em._emphasis("__bold__") == "<strong>bold</strong>"
        assert em._emphasis("*it*") == "<em>it</em>"
        assert em._emphasis("~~gone~~") == "<s>gone</s>"
        # snake_case identifiers must not turn into italics.
        assert em._emphasis("a_b_c") == "a_b_c"

    def test_unclosed_code_fences_are_linear(self) -> None:
        # "(`+)([\s\S]+?)\1": each unclosed fence rescanned the whole tail.
        assert_fast(em._inline, "``" * (HOSTILE // 8) + "a" * HOSTILE)

    def test_unclosed_link_openers_are_linear(self) -> None:
        # Each "[a](" opener rescanned the tail looking for ")".
        assert_fast(em._inline, "[a](" * (HOSTILE // 4))

    def test_unclosed_star_emphasis_is_linear(self) -> None:
        # Every opener scans to EOL; every candidate closer is rejected by the
        # preceding space, so the scan restarts at the next opener.
        assert_fast(em._emphasis, " **a" * (HOSTILE // 4))

    def test_unclosed_underscore_emphasis_is_linear(self) -> None:
        assert_fast(em._emphasis, " __a" * (HOSTILE // 4))


class TestTtsMarkdownStripping:
    """Speech normalisation of message content (alerts #488-#490)."""

    def test_ordinary_markdown_becomes_speakable(self) -> None:
        spoken = tts._strip_markdown_for_speech(
            "## Result\n\n"
            "See ![chart](c.png) and [the run](https://example.invalid/r).\n\n"
            "- first\n"
            "1. second\n\n"
            "| a | b |\n| --- | --- |\n| 1 | 2 |\n\n"
            "```\ncode()\n```\n"
        )
        assert "Result" in spoken
        assert "the run" in spoken and "https://example.invalid/r" not in spoken
        assert "chart" not in spoken  # images drop entirely
        assert "first" in spoken and "second" in spoken
        assert "(code snippet)" in spoken
        assert "|" not in spoken and "#" not in spoken

    def test_unclosed_image_openers_are_linear(self) -> None:
        assert_fast(tts._strip_markdown_for_speech, "![a](" * (HOSTILE // 5))

    def test_unclosed_link_openers_are_linear(self) -> None:
        assert_fast(tts._strip_markdown_for_speech, "[a](" * (HOSTILE // 4))

    def test_leading_marker_whitespace_run_is_linear(self) -> None:
        assert_fast(tts._strip_markdown_for_speech, " \t" * (HOSTILE // 2) + "1x")


class TestKnowledgeNoteParsing:
    """Note titles and section splitting (alerts #465, #486, #484)."""

    def test_h1_title_is_extracted_and_stripped(self) -> None:
        # The trailing-whitespace trim moved out of _H1_RE's tail into the call
        # site; the stored title is unchanged, trailing spaces and all.
        fields = kb_reindex.note_fields("notes/n.md", {}, "# Weekly report  \n\nbody")
        assert fields["title"] == "Weekly report"
        assert kb_reindex.note_fields("notes/n.md", {}, "no heading\n")["title"] == "n"
        assert gardener.note_title("# Weekly report\n\nbody") == "Weekly report"

    def test_sections_split_on_heading_breadcrumbs(self) -> None:
        assert chunker._split_sections("pre\n\n# A\n\ntext\n\n## B\n\nmore") == [
            (None, "pre"),
            ("A", "# A\n\ntext"),
            ("A > B", "## B\n\nmore"),
        ]

    def test_wikilink_targets_drop_aliases_anchors_and_embeds(self) -> None:
        body = "see [[alpha|A]] and ![[embed]] and [[b#s]] and [x](n.md)"
        assert gardener._WIKILINK_RE.findall(body) == ["alpha|A", "b#s"]
        assert gardener._internal_link_targets(body) == ["n", "alpha", "b"]

    def test_h1_trailing_whitespace_run_is_linear(self) -> None:
        # "^#\s+(.+?)\s*$" under MULTILINE: the trailing "\s*" re-split the run.
        assert_fast(kb_reindex._H1_RE.search, "# t" + " " * HOSTILE + "x")

    def test_heading_whitespace_run_is_linear(self) -> None:
        assert_fast(chunker._split_sections, "#" + " \t" * (HOSTILE // 2) + "x")

    def test_unclosed_wikilink_openers_are_linear(self) -> None:
        # Each "[[" rescanned the tail for a closing "]]".
        assert_fast(gardener._WIKILINK_RE.search, "[[" * (HOSTILE // 2))


class TestRepositoryUrls:
    """Repository identities from user-supplied config (alerts #485, #487)."""

    @pytest.mark.parametrize(
        "url,expected",
        [
            ("https://user:pass@github.invalid/o/r.git", True),
            ("git@github.invalid:owner/repo.git", True),  # scp-style
            ("ssh://git@github.invalid/o/r.git", True),
            ("https://github.invalid/owner/repo.git", False),
            ("", False),
        ],
    )
    def test_credential_detection(self, url: str, expected: bool) -> None:
        assert repository_url_has_credentials(url) is expected

    def test_url_masking_hides_only_the_secret(self) -> None:
        masked = GitManager._mask_url_static("https://user:tok@github.invalid/o/r.git")
        assert masked == "https://user:***@github.invalid/o/r.git"
        assert GitManager._mask_url_static("https://github.invalid/o/r.git") == (
            "https://github.invalid/o/r.git"
        )

    def test_at_sign_run_is_linear(self) -> None:
        # "^[^/\\\s]+@[^:]+:": the two runs overlap on every "@".
        assert_fast(repository_url_has_credentials, "a@" * (HOSTILE // 2))

    def test_scheme_marker_run_is_linear(self) -> None:
        # "://([^:]+):[^@]+@": each "://" rescanned the tail for an "@".
        assert_fast(GitManager._mask_url_static, "://a" * (HOSTILE // 4))


@pytest.mark.parametrize(
    "case", ["tilde_fence", "quoted_links", "code_widths", "gardener_links"]
)
def test_remaining_hostile_shapes_have_bounded_growth(case):
    """A subprocess bounds wall time; process CPU measures parser growth.

    The worker can be descheduled while CI runs other tests, which must not
    inflate one growth sample. The 100ms floor absorbs timer noise on fast
    fixed implementations; ratios still distinguish repeated scans.
    """
    import json
    import subprocess
    import sys
    import textwrap

    script = textwrap.dedent("""
        import json, sys, time
        from orchestrator.services import email_markdown as em
        case = sys.argv[1]
        samples = []
        for n in ([100, 200, 400] if case == 'code_widths' else [25000, 50000, 100000]):
            if case == 'tilde_fence':
                text = '~' * n + '`'
                fn = em._FENCE_RE.match
            elif case == 'quoted_links':
                text = '[](! "' * n
                fn = em._inline
            elif case == 'gardener_links':
                from shared.runtime.knowledge import gardener
                text = '[[' * n
                fn = gardener._internal_link_targets
            else:
                text = ''.join('`' * i + 'x' for i in range(1, n)) + 'a' * (n*n)
                fn = lambda text: em._sub_code_spans(text, lambda inner: inner)
            start = time.process_time()
            fn(text)
            samples.append([len(text), time.process_time() - start])
        print(json.dumps(samples))
    """)
    result = subprocess.run(
        [sys.executable, "-c", script, case],
        text=True,
        capture_output=True,
        timeout=12,
        check=True,
    )
    samples = json.loads(result.stdout)
    assert samples[-1][0] >= 100000
    for (size_before, before), (size_after, after) in zip(samples, samples[1:]):
        assert after < 3.0, samples
        assert after <= max(0.1, before * (size_after / size_before) * 1.5), samples


@pytest.mark.parametrize(
    "url",
    [
        "http://[::1]:3000/",
        "https://[2001:db8::1]/notes",
        "https://user:pw@[::1]/notes",
    ],
)
def test_ipv6_links_remain_rendered_and_speakable(url):
    md = f"[dashboard]({url})"
    assert gardener.external_url_map([{"path": "note.md", "text": md}]) == {
        url: ["note.md"]
    }
    assert f'href="{url}"' in em._inline(md)
    assert tts._strip_markdown_for_speech(md) == "dashboard"
    assert "chart" not in tts._strip_markdown_for_speech(f"![chart]({url})")


def test_link_titles_can_contain_brackets_without_rescanning():
    assert ">guide</a>" in em._inline('[guide](https://example.com "notes [draft]")')
    assert ">inner</a>" in em._inline("[broken]( nope [inner](https://example.com)")
    assert ">later</a>" in em._inline(
        '[bad](https://bad "unterminated\n[later](https://example.com)'
    )


def test_code_span_widths_match_exactly_and_preserve_unmatched_runs():
    assert (
        em._sub_code_spans("`a``b` and ``c`d``", lambda inner: "[" + inner + "]")
        == "[a``b] and [c`d]"
    )
    assert (
        em._sub_code_spans("```x ``y`z`` and `done`", lambda inner: "[" + inner + "]")
        == "```x [y`z] and [done]"
    )
