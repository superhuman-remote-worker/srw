"""A job that plans in phases gets byte-identical prompt text.

The worker scaffolds and the engineer and designer instruction files keep the
phased procedure inside ``has_tool("next_phase_todos")`` blocks, for jobs
without phases, which do not bind the phase tools. With the phase tools bound,
every file must render exactly as it did before those blocks were added: the
fixtures in ``tests/fixtures/phased_prompt_golden/`` were written from the
files as they were then (see ``tests/_phased_prompt_golden.py``, which also
says how to regenerate them after a deliberate change).
"""

from __future__ import annotations

import difflib

import pytest

from tests._phased_prompt_golden import (
    PHASE_TOOLS,
    SOURCES,
    load_manifest,
    read_fixture,
    render,
)

_MANIFEST = load_manifest()
_CASES = [
    (profile, source)
    for profile, sources in sorted(_MANIFEST["cases"].items())
    for source in SOURCES
]


def test_golden_covers_every_file_and_binds_the_phase_tools():
    assert len(SOURCES) == 16
    tool_lists = _MANIFEST["tool_lists"]
    assert {"all_tools", "phase_tools_only"} <= set(tool_lists)
    assert any(name.startswith("expert_") for name in tool_lists)
    for profile, tools in tool_lists.items():
        assert "next_phase_todos" in tools, profile
        assert set(_MANIFEST["cases"][profile]) == set(SOURCES), profile
    # Both sides of every other condition are covered by the first two lists.
    assert set(PHASE_TOOLS) <= set(tool_lists["all_tools"])
    assert "kb_write" in tool_lists["all_tools"]
    assert "kb_write" not in tool_lists["phase_tools_only"]


@pytest.mark.parametrize(
    "profile,source", _CASES, ids=[f"{p}:{s.split('/')[-1]}" for p, s in _CASES]
)
def test_phased_render_is_byte_identical(profile, source):
    tools = _MANIFEST["tool_lists"][profile]
    expected = read_fixture(_MANIFEST["cases"][profile][source])
    actual = render(source, tools)
    if actual != expected:
        diff = "".join(
            difflib.unified_diff(
                expected.splitlines(keepends=True),
                actual.splitlines(keepends=True),
                fromfile=f"golden {profile}",
                tofile=f"rendered {source}",
            )
        )
        pytest.fail(f"{source} changed for the phase tools ({profile}):\n{diff}")
