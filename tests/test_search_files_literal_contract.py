"""search_files matches literal text, with one contract on every backend.

knowledge-base/knowledge/issues/search_files_literal_query_contract.md: job
675f630c passed regex-shaped queries to search_files, got "No matches found"
and needed coordinator guidance to use rg in its VM shell. The backends
disagreed underneath the one tool description ("Text or pattern"):

* RemoteBackend (every SSH workspace: pods, VMs, the static pool) ran
  ``grep -rn`` with a POSIX *basic* regex -- ``|``/``+``/``()`` literal, but
  ``.``/``*``/``[...]`` special, so ``x[0]`` never found ``x[0]`` -- and
  without ``-H``, so GNU grep printed no filename for a single-file ``path``
  and the parser dropped every hit;
* the virtual, scratch and test backends matched a plain substring and
  silently ignored ``exclude_dirs``.

The RemoteBackend case runs the backend's real grep command through
``/bin/sh`` against tmp_path, so it checks what grep actually matches rather
than the command string.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

from agent.core.backends.scratch import ScratchBackend
from agent.core.backends.virtual import VirtualWorkspaceBackend
from agent.core.workspace import WorkspaceManager
from agent.tools.context import ToolContext
from agent.tools.workspace.filesystem import create_filesystem_tools
from shared.runtime.core.backends.object_store import InMemoryObjectStore
from shared.runtime.core.backends.remote import RemoteBackend
from tests._fs_backend import FilesystemTestBackend

FILES = {
    "notes/a.txt": "\n".join(
        [
            "alpha foo",
            "beta bar",
            "gamma foo|bar",
            "value = x[0]",
            "x0",
            "a.c",
            "abc",
        ]
    ),
    "node_modules/dep/b.txt": "foo in a dependency",
}


def _write_local(root: Path, rel: str, text: str) -> None:
    target = root / rel
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(text, encoding="utf-8")


def _remote_backend(root: Path, monkeypatch) -> RemoteBackend:
    """A RemoteBackend whose server-side command runs locally under /bin/sh.

    The command runs from ``root``'s parent, standing in for the SSH user's
    home directory -- the cwd a real workspace command starts in.
    """
    backend = RemoteBackend(
        host="127.0.0.1",
        port=22,
        username="agent",
        key_path=str(root / "no-key"),
        workspace_path=str(root),
        job_id="literal-contract",
    )
    monkeypatch.setattr(backend, "_ensure_connected", lambda: None)
    monkeypatch.setattr(
        backend,
        "_exec",
        lambda command, timeout=30: subprocess.run(
            command,
            shell=True,
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=root.parent,
        ).stdout,
    )
    return backend


@pytest.fixture(params=["remote", "virtual", "scratch", "filesystem-test"])
def seeded(request, tmp_path, monkeypatch):
    kind = request.param
    root = tmp_path / "ws"
    root.mkdir()
    if kind == "remote":
        if shutil.which("grep") is None:  # pragma: no cover - every CI image has it
            pytest.skip("grep is not installed")
        backend = _remote_backend(root, monkeypatch)
        for rel, text in FILES.items():
            _write_local(root, rel, text)
    else:
        if kind == "virtual":
            backend = VirtualWorkspaceBackend(InMemoryObjectStore(), prefix="jobs/j/")
        elif kind == "scratch":
            backend = ScratchBackend(job_id="literal", base_dir=str(tmp_path))
        else:
            backend = FilesystemTestBackend(root)
        for rel, text in FILES.items():
            backend.write_file(rel, text)
    yield backend
    if kind == "scratch":
        backend.disconnect()


def _lines(results) -> list[str]:
    return sorted(r["line"] for r in results)


def _paths(results) -> set[str]:
    return {r["path"] for r in results}


def test_alternation_is_literal_text(seeded):
    """The issue's fixture: foo, bar and the literal text foo|bar."""
    assert _lines(seeded.search_files("foo|bar")) == ["gamma foo|bar"]
    assert _lines(seeded.search_files("bar", path="notes")) == [
        "beta bar",
        "gamma foo|bar",
    ]


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        pytest.param("x[0]", ["value = x[0]"], id="brackets-are-not-a-class"),
        pytest.param("a.c", ["a.c"], id="dot-is-not-a-wildcard"),
        pytest.param("foo|bar", ["gamma foo|bar"], id="pipe-is-not-alternation"),
    ],
)
def test_regex_metacharacters_are_plain_text(seeded, query, expected):
    assert _lines(seeded.search_files(query, path="notes")) == expected


def test_case_is_ignored_unless_requested(seeded):
    assert _lines(seeded.search_files("FOO|BAR")) == ["gamma foo|bar"]
    assert seeded.search_files("FOO|BAR", case_sensitive=True) == []


def test_path_scopes_to_a_directory_or_a_single_file(seeded):
    assert _paths(seeded.search_files("foo", path="node_modules")) == {
        "node_modules/dep/b.txt"
    }
    single = seeded.search_files("foo", path="notes/a.txt")
    assert _paths(single) == {"notes/a.txt"}
    assert _lines(single) == ["alpha foo", "gamma foo|bar"]
    assert all(isinstance(r["line_number"], int) for r in single)


def test_exclude_dirs_skips_directory_names_at_any_depth(seeded):
    assert "node_modules/dep/b.txt" in _paths(seeded.search_files("foo"))
    assert _paths(seeded.search_files("foo", exclude_dirs=["node_modules"])) == {
        "notes/a.txt"
    }
    assert _paths(seeded.search_files("foo", exclude_dirs=["dep"])) == {"notes/a.txt"}
    assert _paths(seeded.search_files("foo", exclude_dirs=["node_*"])) == {
        "notes/a.txt"
    }


# ---------------------------------------------------------------------------
# The SSH command treats a model-supplied path as one word of data
# ---------------------------------------------------------------------------


@pytest.fixture
def home(tmp_path, monkeypatch):
    """A 'home' holding the workspace plus a file the workspace must not reach."""
    if shutil.which("grep") is None:  # pragma: no cover
        pytest.skip("grep is not installed")
    root = tmp_path / "ws"
    _write_local(root, "my notes/a.txt", "needle inside the workspace")
    _write_local(tmp_path, "outside/secret.txt", "needle outside the workspace")
    return tmp_path, _remote_backend(root, monkeypatch)


def test_remote_path_with_spaces_is_one_argument(home):
    _, backend = home
    hits = backend.search_files("needle", path="my notes")
    assert _paths(hits) == {"my notes/a.txt"}


def test_remote_path_cannot_reach_outside_the_workspace(home):
    """Unquoted, "x outside" split into a second grep operand resolved from
    the user's home directory -- outside the workspace root."""
    _, backend = home
    assert backend.search_files("needle", path="x outside") == []


def test_remote_path_is_never_shell_syntax(home):
    """search_files is bound on shell-less tiers too; its path must not run
    commands."""
    tmp_path, backend = home
    marker = tmp_path / "injected"
    for path in (f"x; touch {marker}; true", f"x$(touch {marker})", "x`true`"):
        assert backend.search_files("needle", path=path) == []
    assert not marker.exists()


# ---------------------------------------------------------------------------
# The tool surface a worker actually reads
# ---------------------------------------------------------------------------


def _tool(tmp_path, *, bound=("search_files",)):
    """The search tool on a shell-capable backend, with ``bound`` tools loaded.

    A ShellManager is always present, as on every SSH workspace; whether a
    shell TOOL is bound is what the ``bound`` names decide.
    """
    ws = WorkspaceManager(
        job_id="literal-tool",
        base_path=tmp_path,
        backend=FilesystemTestBackend(tmp_path),
    )
    ws.initialize()
    for rel, text in FILES.items():
        ws.write_file(rel, text)
    ctx = ToolContext(workspace_manager=ws, config={"max_search_results": 50})
    ctx.shell_manager = object()
    ctx._resolved_tool_names = list(bound)
    return next(t for t in create_filesystem_tools(ctx) if t.name == "search_files")


def test_schema_states_the_literal_contract(tmp_path):
    description = _tool(tmp_path).description
    assert "literal" in description.lower()
    assert '"foo|bar"' in description
    assert "not a regex" in description.lower()
    assert "exclude_dirs" in description
    assert "case_sensitive" in description


def test_literal_hit_renders_normally(tmp_path):
    out = _tool(tmp_path).invoke({"query": "foo|bar"})
    assert "gamma foo|bar" in out
    assert "alpha foo" not in out


@pytest.mark.parametrize(
    ("bound", "advice", "absent"),
    [
        pytest.param(
            ["search_files", "run_command"], "grep -E", None, id="run_command-bound"
        ),
        pytest.param(
            ["search_files", "shell_execute", "shell_read"],
            "grep -E",
            None,
            id="shell_execute-bound",
        ),
        # writer / general-worker / curator / centurion: search_files with
        # `shell: []` on a shell-capable backend -- a ShellManager, no tool.
        pytest.param(
            ["search_files", "read_file"],
            "each one separately",
            "shell",
            id="shell-backend-without-a-shell-tool",
        ),
        pytest.param(
            ["search_files", "srw_cloud_status"],
            "each one separately",
            "shell",
            id="cloud-status-is-not-a-shell",
        ),
        pytest.param([], "each one separately", "shell", id="not-yet-loaded"),
    ],
)
def test_regex_shaped_miss_says_it_was_literal(tmp_path, bound, advice, absent):
    out = _tool(tmp_path, bound=bound).invoke({"query": "(alpha|beta) fo+"})
    assert out.startswith("No matches found for: (alpha|beta) fo+")
    assert "matched literally" in out
    assert advice in out
    if absent:
        assert absent not in out.lower()


def test_plain_miss_carries_no_regex_note(tmp_path):
    out = _tool(tmp_path, bound=["search_files", "run_command"]).invoke(
        {"query": "zebra"}
    )
    assert out == "No matches found for: zebra"


def test_missing_path_is_not_reported_as_no_matches(tmp_path):
    out = _tool(tmp_path).invoke({"query": "foo", "path": "no/such/dir"})
    assert out == "Error: path not found: no/such/dir"


@pytest.mark.parametrize("query", ["", "alpha\nbeta"])
def test_empty_or_multiline_query_is_refused(tmp_path, query):
    out = _tool(tmp_path).invoke({"query": query})
    assert out.startswith("Error:")
