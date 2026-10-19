"""The permission-mode vocabulary and its legacy spelling."""

import pytest

from shared.permission_modes import (
    ACCEPTED_PERMISSION_MODES,
    DEFAULT_PERMISSION_MODE,
    PERMISSION_MODES,
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
