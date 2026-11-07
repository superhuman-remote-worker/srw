"""list_files lists ``.git/`` but never expands it.

A root listing used to print Git's HEAD, config, refs/ and objects/ under
``.git/`` (SRW vs Claude Code benchmark, 2026-11-04: ~1.7k characters of every
job's first root listing). Listing ``.git`` itself still shows its contents.
"""

from __future__ import annotations

from agent.core.workspace import WorkspaceManager
from agent.tools.context import ToolContext
from agent.tools.workspace import create_workspace_tools
from tests._fs_backend import FilesystemTestBackend


def _list_files(tmp_path):
    root = tmp_path / "ws"
    root.mkdir()
    (root / ".git" / "refs").mkdir(parents=True)
    (root / ".git" / "HEAD").write_text("ref: refs/heads/main\n")
    (root / "output").mkdir()
    (root / "output" / "report.md").write_text("# r\n")
    (root / "README.md").write_text("# w\n")
    ws = WorkspaceManager(
        job_id="list-git", base_path=root, backend=FilesystemTestBackend(root)
    )
    ctx = ToolContext(workspace_manager=ws)
    tools = {t.name: t for t in create_workspace_tools(ctx)}
    return tools["list_files"]


def test_root_listing_shows_git_collapsed(tmp_path):
    list_files = _list_files(tmp_path)

    out = list_files.invoke({"path": "", "depth": 2})

    assert "  .git/\n" in out
    assert "HEAD" not in out
    assert "refs/" not in out
    assert "    report.md" in out
    assert "  README.md" in out


def test_listing_git_itself_shows_its_contents(tmp_path):
    list_files = _list_files(tmp_path)

    out = list_files.invoke({"path": ".git"})

    assert "HEAD" in out
    assert "refs/" in out
