"""Citation handles and the instant quote check (citation engine overhaul).

The handle is what the agent writes (``[^c1241]``); readers see footnote
numbers assigned at render time. The quote check is the deterministic half of
verification: a ``direct_quote`` whose words are not in the source fails at
once, without an LLM.
"""

import pytest

from agent.citation_engine.engine import find_quote_in_source
from shared.citation_handles import (
    format_handle,
    format_source_ref,
    handles_in,
    parse_citation_ref,
    parse_source_ref,
)


class TestHandles:
    def test_format(self):
        assert format_handle(1241) == "[^c1241]"
        assert format_source_ref(12) == "S12"

    @pytest.mark.parametrize(
        "value",
        [1241, "1241", "[1241]", "c1241", "^c1241", "[^c1241]", " [^c1241] ", "C1241"],
    )
    def test_parse_accepts_the_forms_models_write(self, value):
        assert parse_citation_ref(value) == 1241

    @pytest.mark.parametrize("value", ["", "abc", "S12", "[^c]", True, "12 13"])
    def test_parse_rejects_non_handles(self, value):
        with pytest.raises(ValueError):
            parse_citation_ref(value)

    @pytest.mark.parametrize("value", [12, "12", "S12", "s12", "[S12]", "[12]"])
    def test_parse_source(self, value):
        assert parse_source_ref(value) == 12

    def test_handles_in_first_mention_order(self):
        text = "B [^c7] and A [^c3], again B [^c7] and C [c9]."
        assert handles_in(text) == [7, 3, 9]

    def test_handles_in_skips_code_and_definitions(self):
        text = (
            "Claim [^c2].\n\n"
            "```\nexample [^c99]\n```\n"
            "Inline `[^c98]` is literal.\n\n"
            "[^c2]: Source, page 4 [^c97]\n"
        )
        assert handles_in(text) == [2]

    def test_legacy_markers_are_not_handles(self):
        # Old sessions keep [1241] (D5); only the session chat resolves those.
        assert handles_in("old style [1241] marker") == []


class TestQuoteCheck:
    SOURCE = (
        "Section 3.\nCompanies must retain   accounting records for ten "
        "years.\nThe regu-\nlation applies to “all” books."
    )

    def test_exact_quote_is_found_and_returned(self):
        found = find_quote_in_source("must retain accounting records", self.SOURCE)
        assert found is not None
        assert found.startswith("must retain")
        assert found.endswith("records")

    def test_spacing_case_punctuation_and_line_breaks_may_differ(self):
        assert find_quote_in_source(
            'COMPANIES must retain accounting records for ten years. The regulation applies to "all" books',
            self.SOURCE,
        )

    def test_ellipsis_fragments_must_appear_in_order(self):
        assert find_quote_in_source(
            "Companies must retain … for ten years", self.SOURCE
        )
        assert (
            find_quote_in_source(
                "for ten years [...] Companies must retain", self.SOURCE
            )
            is None
        )

    def test_made_up_quote_is_not_found(self):
        assert (
            find_quote_in_source(
                "records must be destroyed after one year", self.SOURCE
            )
            is None
        )

    def test_short_quote_is_left_to_the_verifier(self):
        # Too few letters to say anything: treated as found.
        assert find_quote_in_source("ten", "nothing like it") == "ten"

    def test_nfkc_ligatures_compare_equal(self):
        assert find_quote_in_source(
            "financial statements", "ﬁnancial statements are due"
        )


class TestFootnoteBlock:
    def test_appends_then_replaces_never_doubles(self):
        from shared.citation_handles import DEFINITIONS_START, replace_definitions

        text = "Claim [^c2].\n"
        once = replace_definitions(text, ["[^c2]: Source A."])
        assert once == (
            "Claim [^c2].\n\n<!-- srw:citations -->\n[^c2]: Source A.\n"
            "<!-- /srw:citations -->\n"
        )
        twice = replace_definitions(once + "More [^c3].\n", ["[^c2]: A.", "[^c3]: B."])
        assert twice.count(DEFINITIONS_START) == 1
        assert "More [^c3]." in twice
        assert twice.rstrip().endswith("<!-- /srw:citations -->")
        assert replace_definitions(twice, []) == "Claim [^c2].\n\nMore [^c3].\n"

    def test_definition_lines_flag_state(self):
        from datetime import datetime, timezone
        from types import SimpleNamespace

        from agent.citation_engine.delivery import definition

        def cit(status, **kw):
            base = dict(
                locator={"page": 4},
                verbatim_quote="must retain records",
                quote_context="",
                verification_status=SimpleNamespace(value=status),
                verification_reason=None,
                withdrawn_at=None,
                withdrawn_reason=None,
            )
            base.update(kw)
            return SimpleNamespace(**base)

        web = SimpleNamespace(name="Rules", identifier="https://example.org/r")
        assert definition(5, cit("verified"), web) == (
            "[^c5]: Rules, <https://example.org/r>, p. 4. “must retain records”"
        )
        failed = definition(
            6, cit("failed", verification_reason="quote_not_found"), web
        )
        assert failed.endswith(
            "**Unverified:** the quoted words are not in the source."
        )
        withdrawn = definition(
            7,
            cit(
                "failed",
                withdrawn_at=datetime.now(timezone.utc),
                withdrawn_reason="dropped",
            ),
            web,
        )
        assert withdrawn.endswith("**Withdrawn:** dropped.")
        assert (
            definition(8, None, None) == "[^c8]: **Citation not found** for this job."
        )


class TestQuoteCheckOnScrapedPages:
    # The stored text of https://en.wikipedia.org/wiki/Eiffel_Tower as a web
    # provider returned it (k3d smoke 2026-10-29): the agent quoted the
    # sentence as a reader sees it, which the first matcher flagged.
    WIKI = (
        "The tower is 330 metres (1,083 ft) tall,[[9]](#cite_note-9) about the "
        "same height as an 81-storey building, and the [tallest structure in "
        "Paris](https://en.wikipedia.org/wiki/List_of_tallest_buildings_and_"
        'structures_in_the_Paris_region "List of tallest buildings") since its '
        "completion.<sup>[a]</sup> ![logo](https://x.example/a_(b).png)"
    )

    def test_links_and_footnote_markers_are_not_words(self):
        assert find_quote_in_source(
            "The tower is 330 metres (1,083 ft) tall, about the same height as "
            "an 81-storey building, and the tallest structure in Paris.",
            self.WIKI,
        )

    def test_a_quote_copied_with_its_markup_still_matches(self):
        assert find_quote_in_source(
            "The tower is 330 metres (1,083 ft) tall,[[9]](#cite_note-9) about the same",
            self.WIKI,
        )

    def test_changed_words_still_fail(self):
        assert (
            find_quote_in_source(
                "The tower is 300 metres tall and the tallest structure in Lyon.",
                self.WIKI,
            )
            is None
        )
