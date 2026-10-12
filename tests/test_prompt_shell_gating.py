"""Shell guidance in prompts must track the tools actually bound.

Regression cover for a session that called `shell_execute` on a `virtual`
workspace and got `Tool 'shell_execute' not found`. The backend capability gate
(`filter_tools_by_backend`) had correctly dropped the whole shell category, but
the prompt still told the model to "reuse existing shell tabs" — so it called a
tool that was never offered, burning a turn (and, in supervised mode, a
permission card) on a tool that does not exist.

These run against the *shipped* templates rather than synthetic ones: a
synthetic template would not have caught the original bug.
"""

import glob
import re

import pytest

from shared.runtime.core.loader import _has_shell_tools, render_instruction_content

# summarization_* are rendered with .format_map (see services/auxiliary.py),
# not Jinja — they are not part of this contract.
# The worker's tactical guidance is a phase_start-bound skill since U2 (the
# bundled body plus every expert-local override); its shell block must track
# the bound tools exactly like the templates.
PROMPT_FILES = sorted(
    [
        f
        for f in glob.glob("config/prompts/*.txt")
        if not f.split("/")[-1].startswith("summarization_")
    ]
    + glob.glob("config/skills/tactical-phase/SKILL.md")
    + glob.glob("config/experts/*/skills/tactical-phase/SKILL.md")
)

# Tool sets a lite-tier (virtual/none) agent vs. a sandbox agent actually gets.
NO_SHELL = ["read_file", "write_file", "kb_write", "cite_web", "todo_complete"]
WITH_SHELL = NO_SHELL + ["run_command", "shell_read"]

# Phrases that only make sense with a shell bound. Deliberately covers each
# template family's own wording — codex_spark says "existing tabs" where the
# others say "shell tabs", and a probe list that misses a variant would let that
# family regress silently.
SHELL_PHRASES = (
    "shell tab",
    "existing tabs",
    "Shell management",
    "Shell Management",
    "shell_management",
    "# Shell",
    "run_command",
    "shell commands",
)


def _render(path, tools, **kwargs):
    with open(path) as fh:
        return render_instruction_content(fh.read(), tools, **kwargs)


def _strip_raw(text):
    """Drop {% raw %} payloads — they hold literal {{ }} for gemma's wire format."""
    return re.sub(r"\{%\s*raw\s*%\}.*?\{%\s*endraw\s*%\}", "", text, flags=re.S)


def test_prompt_files_discovered():
    """Guard against the glob silently matching nothing."""
    assert len(PROMPT_FILES) > 20
    assert "config/skills/tactical-phase/SKILL.md" in PROMPT_FILES
    assert sum(p.startswith("config/experts/") for p in PROMPT_FILES) >= 6


@pytest.mark.parametrize("path", PROMPT_FILES)
def test_renders_without_shell(path):
    """No template may leak shell guidance when no shell tool is bound."""
    out = _render(path, NO_SHELL)
    for phrase in SHELL_PHRASES:
        assert phrase not in out, f"{path} mentions '{phrase}' with no shell bound"


@pytest.mark.parametrize("path", PROMPT_FILES)
def test_renders_cleanly_both_ways(path):
    """Every template must render for both tool sets, leaving no markers behind."""
    for tools in (NO_SHELL, WITH_SHELL):
        out = _render(path, tools)
        assert "{%" not in _strip_raw(out), f"{path} left an unrendered Jinja marker"


def test_shell_blocks_return_when_shell_is_bound():
    """The gate must not be one-way — bind a shell and the guidance comes back."""
    gated = [p for p in PROMPT_FILES if "has_shell" in open(p).read()]
    assert gated, "no template gates on has_shell — did the gating regress?"
    for path in gated:
        out = _render(path, WITH_SHELL)
        assert any(p in out for p in SHELL_PHRASES), (
            f"{path} suppresses shell guidance even with a shell bound"
        )


class TestHasShellTools:
    def test_execution_tools_count(self):
        for name in ("run_command", "shell_execute", "shell_read", "cancel_command"):
            assert _has_shell_tools({name}), f"{name} should imply a shell"

    def test_non_shell_tools_do_not_count(self):
        assert not _has_shell_tools({"read_file", "web_search"})
        assert not _has_shell_tools(set())

    def test_cloud_status_does_not_imply_a_shell(self):
        """`srw_cloud_status` is shell-category but grant:"code".

        It is re-appended *after* filter_tools_by_backend whenever a cloud mount
        is active, so it is bound on shell-less tiers too. Counting it would
        re-open the shell blocks on exactly the lite-tier sessions this gate
        protects.
        """
        assert not _has_shell_tools({"srw_cloud_status"})
        assert not _has_shell_tools({"read_file", "srw_cloud_status"})
