"""The write guard: a redacted view must never be written back to disk.

Tool results the model reads are credential-redacted
(``agent.core.tool_output_redaction``), so a token in a file comes back as
``[REDACTED]``. The agent edits files from what it read: an ``old_string``
copied from that view no longer matches, and a whole-file ``write_file`` of it
would replace the real value with the marker — silently, since every later
read and diff is redacted the same way. ``write_file`` and ``edit_file``
refuse content that carries the marker unless the file on disk already does,
and say why, so the agent edits around the span it cannot see.
"""

from __future__ import annotations

import pytest

# Synthetic, never a real credential: token-shaped, so the tool profile
# redacts it inside a URL.
TOKEN = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b"
REMOTE = f"https://oauth2:{TOKEN}@gitea.local/org/repo.git"


@pytest.fixture
def file_tools(tmp_path):
    from agent.core.workspace import WorkspaceManager
    from agent.tools.context import ToolContext
    from agent.tools.workspace.files import create_file_tools
    from tests._fs_backend import FilesystemTestBackend

    workspace = WorkspaceManager(
        job_id="t", base_path=tmp_path, backend=FilesystemTestBackend(tmp_path)
    )
    context = ToolContext(workspace_manager=workspace)
    context._llm_config = None
    tools = {t.name: t for t in create_file_tools(context)}

    def call(name, **kwargs):
        from agent.core.tool_output_redaction import redact_tool_result

        return redact_tool_result(tools[name].invoke(kwargs), context)

    return workspace, tmp_path, call


def _visible(read_result: str) -> str:
    """The file text as the model saw it, line-number gutter stripped."""
    return "\n".join(
        line.split("\t", 1)[1] for line in read_result.splitlines() if "\t" in line
    )


class TestWriteGuard:
    BODY = f"[remote]\nurl = {REMOTE}\nname = origin\n"

    def test_rewriting_a_redacted_file_is_refused_and_disk_is_untouched(
        self, file_tools
    ):
        workspace, root, call = file_tools
        workspace.write_file("git.cfg", self.BODY)
        seen = _visible(call("read_file", path="git.cfg"))
        assert "[REDACTED]" in seen and TOKEN not in seen

        result = call("write_file", path="git.cfg", content=seen + "\nextra = 1\n")
        assert result.startswith("Error: write_file refused")
        assert "redacted" in result
        assert (root / "git.cfg").read_text() == self.BODY

    def test_an_old_string_copied_from_the_redacted_view_explains_itself(
        self, file_tools
    ):
        workspace, root, call = file_tools
        workspace.write_file("git.cfg", self.BODY)
        seen = _visible(call("read_file", path="git.cfg"))
        marker_line = next(line for line in seen.splitlines() if "[REDACTED]" in line)
        result = call(
            "edit_file",
            path="git.cfg",
            old_string=marker_line,
            new_string=marker_line.replace("origin", "upstream"),
        )
        assert result.startswith("Error:")
        assert "redacted" in result.lower()
        assert (root / "git.cfg").read_text() == self.BODY

    def test_an_edit_that_would_add_the_marker_is_refused(self, file_tools):
        workspace, root, call = file_tools
        workspace.write_file("git.cfg", self.BODY)
        call("read_file", path="git.cfg")
        result = call(
            "edit_file",
            path="git.cfg",
            old_string="name = origin",
            new_string="name = origin\nbackup = https://[REDACTED]@mirror/x.git",
        )
        assert result.startswith("Error: edit_file refused")
        assert (root / "git.cfg").read_text() == self.BODY

    def test_an_edit_beside_the_redacted_span_goes_through(self, file_tools):
        workspace, root, call = file_tools
        workspace.write_file("git.cfg", self.BODY)
        call("read_file", path="git.cfg")
        result = call(
            "edit_file",
            path="git.cfg",
            old_string="name = origin",
            new_string="name = upstream",
        )
        assert result.startswith("Edited")
        assert (root / "git.cfg").read_text() == self.BODY.replace("origin", "upstream")

    def test_a_file_that_already_holds_the_marker_can_be_written(self, file_tools):
        # This repository's own redaction tests contain the literal marker.
        workspace, root, call = file_tools
        workspace.write_file("t.py", 'assert out == "[REDACTED]"\n')
        call("read_file", path="t.py")
        body = 'assert out == "[REDACTED]"\nassert n == 1\n'
        assert call("write_file", path="t.py", content=body).startswith("Written")
        assert (root / "t.py").read_text() == body

    def test_a_new_file_quoting_the_marker_is_refused(self, file_tools):
        _workspace, root, call = file_tools
        result = call(
            "write_file", path="notes.md", content="remote: https://[REDACTED]@h/x"
        )
        assert result.startswith("Error: write_file refused")
        assert not (root / "notes.md").exists()


class TestAMarkerAlreadyOnDiskLicensesNothingMore:
    """The review's rw2 case: a fixture file holding the literal marker next
    to a token-shaped URL. Presence alone let the redacted view be written
    back and the real URL was lost; the guard counts markers instead."""

    TOKURL = "https://ci-bot:Zx81q7Lm2Pw9Rt4Vb6Nc@git.example.com/o/r.git"
    BODY = (
        'EXPECTED = "[REDACTED]"  # what the log redactor prints\n'
        f'FIXTURE_REMOTE = "{TOKURL}"\n'
        "def test_x():\n    assert True\n"
    )

    def test_a_whole_file_rewrite_is_refused(self, file_tools):
        workspace, root, call = file_tools
        workspace.write_file("test_logs.py", self.BODY)
        seen = _visible(call("read_file", path="test_logs.py")) + "\n"
        assert seen.count("[REDACTED]") == 2  # the literal AND the redacted URL
        result = call(
            "write_file",
            path="test_logs.py",
            content=seen.replace("assert True", "assert 1 == 1"),
        )
        assert result.startswith("Error: write_file refused")
        assert (root / "test_logs.py").read_text() == self.BODY

    def test_an_edit_spanning_the_redacted_line_is_refused(self, file_tools):
        workspace, root, call = file_tools
        workspace.write_file("t2.py", self.BODY)
        seen = _visible(call("read_file", path="t2.py"))
        line = next(line for line in seen.splitlines() if "FIXTURE_REMOTE" in line)
        result = call(
            "edit_file",
            path="t2.py",
            old_string=line,
            new_string=line.replace("o/r", "o/r2"),
        )
        assert result.startswith("Error:") and "redacted" in result.lower()
        assert (root / "t2.py").read_text() == self.BODY

    def test_appending_one_more_marker_is_refused(self, file_tools):
        workspace, root, call = file_tools
        workspace.write_file("t3.py", self.BODY)
        call("read_file", path="t3.py")
        result = call(
            "edit_file",
            path="t3.py",
            new_string='OTHER = "[REDACTED]"\n',
            position="end",
        )
        assert result.startswith("Error: edit_file refused")
        assert (root / "t3.py").read_text() == self.BODY
