"""Every catalog tool has a Cockpit card title, every tool category a noun.

The Cockpit names a tool card by ``toolCard.titles.<tool>`` and counts a
folded run of calls by ``chat.categoryNouns[One].<category>``. A missing key
still renders (a prettified fallback), but Transloco logs it on every card and
a German Cockpit shows the English fallback. The keys are dynamic, so the
Cockpit's static i18n spec cannot see them; the tool catalog lives here.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from shared.tool_catalog import TOOL_REGISTRY

ROOT = Path(__file__).parent.parent
I18N = ROOT / "cockpit" / "src" / "assets" / "i18n"
LOCALES = ("en", "de-DE")


def _locale(name: str) -> dict:
    return json.loads((I18N / f"{name}.json").read_text(encoding="utf-8"))


def _categories() -> set[str]:
    return {
        str(meta["category"]) for meta in TOOL_REGISTRY.values() if meta.get("category")
    }


def test_catalog_is_not_empty() -> None:
    assert len(TOOL_REGISTRY) > 100
    assert len(_categories()) > 10


@pytest.mark.parametrize("locale", LOCALES)
def test_every_catalog_tool_has_a_card_title(locale: str) -> None:
    titles = _locale(locale)["toolCard"]["titles"]
    missing = sorted(name for name in TOOL_REGISTRY if not titles.get(name))
    assert missing == [], f"{locale}.json lacks toolCard.titles for {missing}"


@pytest.mark.parametrize("locale", LOCALES)
@pytest.mark.parametrize("block", ("categoryNouns", "categoryNounsOne"))
def test_every_tool_category_has_a_noun(locale: str, block: str) -> None:
    nouns = _locale(locale)["chat"][block]
    missing = sorted(category for category in _categories() if not nouns.get(category))
    assert missing == [], f"{locale}.json lacks chat.{block} for {missing}"
