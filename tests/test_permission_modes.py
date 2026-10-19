"""The permission-mode vocabulary and its legacy spelling."""

import pytest

from shared.permission_modes import (
    ACCEPTED_PERMISSION_MODES,
    DEFAULT_PERMISSION_MODE,
    PERMISSION_MODES,
    TOOL_EFFECTS,
    UNDECLARED_EFFECT,
    effect_asks,
    is_permission_mode,
    legacy_wire_permission_mode,
    normalize_permission_mode,
)


def test_order_runs_from_most_to_least_restrictive():
    assert PERMISSION_MODES == ("read_only", "auto_accept", "autonomous")
    assert DEFAULT_PERMISSION_MODE == "read_only"


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("supervised", "read_only"),
        ("read_only", "read_only"),
        ("auto_accept", "auto_accept"),
        ("autonomous", "autonomous"),
        ("bogus", "bogus"),
        ("", ""),
        (None, None),
        (3, 3),
    ],
)
def test_normalize(value, expected):
    assert normalize_permission_mode(value) == expected


def test_accepted_spellings():
    assert ACCEPTED_PERMISSION_MODES == {
        "read_only",
        "supervised",
        "auto_accept",
        "autonomous",
    }
    assert all(is_permission_mode(v) for v in ACCEPTED_PERMISSION_MODES)
    assert not is_permission_mode("bogus")
    assert not is_permission_mode(None)


@pytest.mark.parametrize(
    ("mode", "wire"),
    [
        ("read_only", "supervised"),
        ("supervised", "supervised"),
        ("auto_accept", "auto_accept"),
        ("autonomous", "autonomous"),
    ],
)
def test_legacy_wire_spelling(mode, wire):
    assert legacy_wire_permission_mode(mode) == wire


# The D8 matrix: which effects ask first in which mode.
_ASKS = {
    "read_only": {"write", "external", "shell"},
    "auto_accept": {"external", "shell"},
    "autonomous": set(),
}


@pytest.mark.parametrize("mode", PERMISSION_MODES)
@pytest.mark.parametrize("effect", TOOL_EFFECTS)
def test_effect_asks_matrix(mode, effect):
    assert effect_asks(mode, effect) is (effect in _ASKS[mode])


def test_the_ladder_is_monotonic():
    for effect in TOOL_EFFECTS:
        asks = [effect_asks(mode, effect) for mode in PERMISSION_MODES]
        assert asks == sorted(asks, reverse=True), effect


def test_the_legacy_mode_spelling_gates_like_read_only():
    for effect in TOOL_EFFECTS:
        assert effect_asks("supervised", effect) is effect_asks("read_only", effect)


@pytest.mark.parametrize("effect", TOOL_EFFECTS)
def test_an_unknown_mode_asks_for_everything(effect):
    assert effect_asks("bogus", effect) is True
    assert effect_asks(None, effect) is True


def test_an_unknown_effect_is_treated_as_external():
    assert UNDECLARED_EFFECT == "external"
    for mode in PERMISSION_MODES:
        assert effect_asks(mode, "sideways") is effect_asks(mode, "external")
        assert effect_asks(mode, None) is effect_asks(mode, "external")
