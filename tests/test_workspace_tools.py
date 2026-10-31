"""Unit tests for workspace tools read-before-write discipline.

Tests the read tracking mechanism and enforcement in workspace tools.
"""

import io
import pytest
import tempfile
import zipfile
from pathlib import Path

# Import from src package
from agent.core.workspace import WorkspaceManager  # noqa: E402
from agent.tools.context import ToolContext  # noqa: E402
from agent.tools.workspace import create_workspace_tools  # noqa: E402
from tests._fs_backend import FilesystemTestBackend  # noqa: E402


@pytest.fixture
def temp_workspace():
    """Create a temporary directory for workspace testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def workspace_manager(temp_workspace):
    """Create a WorkspaceManager with a temporary base path."""
    ws = WorkspaceManager(
        job_id="test-job-123",
        base_path=temp_workspace,
        backend=FilesystemTestBackend(temp_workspace),
    )
    ws.initialize()
    return ws


@pytest.fixture
def tool_context(workspace_manager):
    """Create a ToolContext for testing."""
    return ToolContext(workspace_manager=workspace_manager)


@pytest.fixture
def workspace_tools(tool_context):
    """Create workspace tools."""
    tools = create_workspace_tools(tool_context)
    # Convert list to dict for easy access
    return {tool.name: tool for tool in tools}


class TestReadTracking:
    """Tests for ToolContext read tracking."""

    def test_record_file_read(self, tool_context):
        """Test that file reads are recorded in context."""
        tool_context.record_file_read("test.md")
        assert tool_context.was_recently_read("test.md")

    def test_read_tracking_normalizes_path(self, tool_context):
        """Test that paths are normalized for tracking."""
        tool_context.record_file_read("/test.md")
        assert tool_context.was_recently_read("test.md")
        assert tool_context.was_recently_read("/test.md")

    def test_read_tracking_deque_limit(self, tool_context):
        """Test that read tracking respects the deque limit."""
        # Record 15 files (more than the default 10)
        for i in range(15):
            tool_context.record_file_read(f"file_{i}.md")

        # First 5 should be pushed out
        for i in range(5):
            assert not tool_context.was_recently_read(f"file_{i}.md")

        # Last 10 should still be tracked
        for i in range(5, 15):
            assert tool_context.was_recently_read(f"file_{i}.md")

    def test_re_reading_moves_to_end(self, tool_context):
        """Test that re-reading a file moves it to the end of the deque."""
        # Fill deque with 10 files
        for i in range(10):
            tool_context.record_file_read(f"file_{i}.md")

        # Re-read file_0 (should move to end)
        tool_context.record_file_read("file_0.md")

        # Add 9 more files - should push out files 1-9 but not file_0
        for i in range(10, 19):
            tool_context.record_file_read(f"file_{i}.md")

        # file_0 should still be there (was moved to end)
        assert tool_context.was_recently_read("file_0.md")

        # files 1-9 should be gone
        for i in range(1, 10):
            assert not tool_context.was_recently_read(f"file_{i}.md")


class TestReadFileTracking:
    """Tests for read_file recording reads."""

    def test_read_file_records_path(
        self, workspace_tools, workspace_manager, tool_context
    ):
        """Test that read_file records the path in context."""
        # Create a test file
        workspace_manager.write_file("test.md", "Hello, world!")

        # Read the file
        read_file = workspace_tools["read_file"]
        result = read_file.invoke({"path": "test.md"})

        # Check the read was recorded
        assert tool_context.was_recently_read("test.md")
        assert "Hello, world!" in result

    def test_read_file_error_reports_resolved_path_and_search_hint(
        self,
        workspace_tools,
        workspace_manager,
        tool_context,
    ):
        """Test that failed reads are not recorded."""
        read_file = workspace_tools["read_file"]
        result = read_file.invoke({"path": "nonexistent.md"})

        assert "Error" in result
        assert str(workspace_manager.get_path("nonexistent.md")) in result
        assert 'you passed "nonexistent.md"' in result
        assert "search_files" in result
        assert not tool_context.was_recently_read("nonexistent.md")

    def test_read_file_race_uses_same_resolved_not_found_message(
        self,
        workspace_tools,
        workspace_manager,
        monkeypatch,
    ):
        """A file removed after exists() gets the same actionable error."""
        monkeypatch.setattr(workspace_manager, "exists", lambda _path: True)

        result = workspace_tools["read_file"].invoke({"path": "vanished.txt"})

        assert str(workspace_manager.get_path("vanished.txt")) in result
        assert "search_files" in result


class TestReadFileBinaryAndArchiveHandling:
    """read_file must diagnose binary content honestly instead of leaking a
    raw UTF-8 codec error.

    Regression coverage for knowledge-base/knowledge/issues/session_uploads_never_extract_archives.md:
    a zip attached to a session was unreadable because read_file's only
    fallback for undecodable bytes was `f"Error: {str(e)}"` — the bare
    UnicodeDecodeError message.
    """

    def _make_zip(self, entries: dict[str, bytes]) -> bytes:
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            for name, data in entries.items():
                zf.writestr(name, data)
        return buf.getvalue()

    def test_read_file_on_zip_returns_entry_listing(
        self, workspace_tools, workspace_manager, tool_context
    ):
        data = self._make_zip(
            {
                "cover_letter.txt": b"Dear hiring manager...",
                "photo.jpg": b"\xff\xd8\xff\xe0fakejpegbytes",
            }
        )
        workspace_manager.backend.write_file("bundle.zip", data)

        result = workspace_tools["read_file"].invoke({"path": "bundle.zip"})

        assert "cover_letter.txt" in result
        assert "photo.jpg" in result
        assert "2" in result  # entry count
        assert "codec" not in result
        assert "utf-8" not in result.lower()
        assert not result.startswith("Error")
        assert tool_context.was_recently_read("bundle.zip")

    def test_read_file_on_zip_reports_entry_sizes(
        self, workspace_tools, workspace_manager
    ):
        data = self._make_zip({"notes.txt": b"x" * 1234})
        workspace_manager.backend.write_file("notes.zip", data)

        result = workspace_tools["read_file"].invoke({"path": "notes.zip"})

        assert "1,234" in result or "1234" in result

    def test_read_file_on_zip_skips_directories_dotfiles_and_macosx(
        self, workspace_tools, workspace_manager
    ):
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("real.txt", b"hello")
            zf.writestr("dir/", b"")
            zf.writestr(".hidden", b"secret")
            zf.writestr("__MACOSX/._real.txt", b"junk")
        workspace_manager.backend.write_file("mixed.zip", buf.getvalue())

        result = workspace_tools["read_file"].invoke({"path": "mixed.zip"})

        assert "real.txt" in result
        assert ".hidden" not in result
        assert "__MACOSX" not in result

    def test_read_file_on_corrupt_zip_returns_binary_message_not_codec_error(
        self, workspace_tools, workspace_manager
    ):
        garbage = b"this is not actually a zip file, just some bytes\xdd\xdd"
        workspace_manager.backend.write_file("broken.zip", garbage)

        result = workspace_tools["read_file"].invoke({"path": "broken.zip"})

        assert result == f"[binary file: broken.zip, {len(garbage)} bytes]"
        assert "codec" not in result

    def test_read_file_on_refused_zip_surfaces_the_upload_seams_note(
        self, workspace_tools, workspace_manager
    ):
        """Review finding 2: a zip the session-upload seam refused to
        extract (cap/traversal) still *parses fine* as a zip, so without
        checking for the sidecar note, read_file would show a normal,
        successful-looking entry listing with nothing indicating that none
        of these members are actually separately readable — the exact dead
        end from the motivating incident, recreated one layer down.
        """
        from agent.tools.workspace.files import ZIP_REFUSAL_NOTE_SUFFIX

        data = self._make_zip({"cover_letter.txt": b"...", "photos/big.bin": b"..."})
        workspace_manager.backend.write_file("bundle.zip", data)
        workspace_manager.backend.write_file(
            f"bundle.zip{ZIP_REFUSAL_NOTE_SUFFIX}",
            b"Extraction refused: uncompressed contents exceed the "
            b"314572800-byte total limit\n\nThis archive's original bytes "
            b"are stored as-is.",
        )

        result = workspace_tools["read_file"].invoke({"path": "bundle.zip"})

        # The refusal is front and center, not buried or absent.
        assert result.startswith("Extraction refused:")
        assert "314572800-byte total limit" in result
        # The (still accurate) entry listing follows it rather than being
        # replaced — the agent can still see what's inside.
        assert "cover_letter.txt" in result
        assert "photos/big.bin" in result

    def test_read_file_on_ordinary_zip_has_no_refusal_note_prefix(
        self, workspace_tools, workspace_manager
    ):
        """No sidecar note present (the overwhelming majority of zips,
        including every one that isn't a refused session upload) — read_file
        must not invent a refusal that didn't happen."""
        data = self._make_zip({"a.txt": b"hello"})
        workspace_manager.backend.write_file("normal.zip", data)

        result = workspace_tools["read_file"].invoke({"path": "normal.zip"})

        assert "Extraction refused" not in result
        assert "a.txt" in result

    def test_read_file_on_arbitrary_binary_returns_binary_message(
        self, workspace_tools, workspace_manager, tool_context
    ):
        data = b"\xff\xfe\x00\x01\x02binary\xdd\xdd"
        workspace_manager.backend.write_file("blob.dat", data)

        result = workspace_tools["read_file"].invoke({"path": "blob.dat"})

        assert result == f"[binary file: blob.dat, {len(data)} bytes]"
        assert "codec" not in result
        assert tool_context.was_recently_read("blob.dat")

    def test_read_file_on_text_file_is_unaffected(
        self, workspace_tools, workspace_manager
    ):
        """Plain UTF-8 content must still go through the normal text path."""
        workspace_manager.write_file("plain.txt", "hello world")

        result = workspace_tools["read_file"].invoke({"path": "plain.txt"})

        assert "hello world" in result
        assert "binary file" not in result

    def test_read_file_on_docx_is_not_reported_as_binary(
        self, workspace_tools, workspace_manager
    ):
        """The near-miss the incident report calls out: DOCX must keep
        working exactly as before — only archives/other binaries are new."""
        docx = pytest.importorskip("docx")

        document = docx.Document()
        document.add_paragraph("Hello from a real docx.")
        buf = io.BytesIO()
        document.save(buf)
        workspace_manager.backend.write_file("letter.docx", buf.getvalue())

        result = workspace_tools["read_file"].invoke({"path": "letter.docx"})

        assert "binary file" not in result
        assert "Hello from a real docx" in result


class TestEditFileReadRequirement:
    """Tests for edit_file requiring recent read."""

    def test_edit_file_requires_recent_read(self, workspace_tools, workspace_manager):
        """Test that edit_file fails without recent read."""
        # Create a test file
        workspace_manager.write_file("test.md", "Hello, world!")

        # Try to edit without reading first
        edit_file = workspace_tools["edit_file"]
        result = edit_file.invoke(
            {"path": "test.md", "old_string": "Hello", "new_string": "Goodbye"}
        )

        assert "Error" in result
        assert "read_file" in result.lower()

    def test_missing_file_reports_resolved_path_and_search_hint(
        self,
        workspace_tools,
        workspace_manager,
    ):
        result = workspace_tools["edit_file"].invoke(
            {
                "path": "missing.md",
                "old_string": "old",
                "new_string": "new",
            }
        )

        assert str(workspace_manager.get_path("missing.md")) in result
        assert "search_files" in result

    def test_edit_file_works_after_read(self, workspace_tools, workspace_manager):
        """Test that edit_file works after reading."""
        # Create a test file
        workspace_manager.write_file("test.md", "Hello, world!")

        # Read first
        read_file = workspace_tools["read_file"]
        read_file.invoke({"path": "test.md"})

        # Now edit should work
        edit_file = workspace_tools["edit_file"]
        result = edit_file.invoke(
            {"path": "test.md", "old_string": "Hello", "new_string": "Goodbye"}
        )

        assert "Edited" in result

        # Verify the change
        content = workspace_manager.read_file("test.md")
        assert "Goodbye, world!" in content

    def test_edit_file_requires_fresh_read_after_out_of_band_change(
        self, workspace_tools, workspace_manager, tool_context
    ):
        workspace_manager.write_file("test.md", "Agent observed this")
        workspace_tools["read_file"].invoke({"path": "test.md"})

        # Simulate a Canvas/user save while the WS invalidation is unavailable.
        workspace_manager.write_file("test.md", "User changed this")
        result = workspace_tools["edit_file"].invoke(
            {
                "path": "test.md",
                "old_string": "User",
                "new_string": "Agent",
            }
        )

        assert "read_file" in result.lower()
        assert workspace_manager.read_file("test.md") == "User changed this"
        assert not tool_context.was_recently_read("test.md")


class TestEditFilePositionModes:
    """Tests for edit_file position parameter (append/prepend)."""

    def test_edit_file_position_end_appends(self, workspace_tools, workspace_manager):
        """Test that position='end' appends to file."""
        # Create a test file
        workspace_manager.write_file("test.md", "Line 1")

        # Read first
        read_file = workspace_tools["read_file"]
        read_file.invoke({"path": "test.md"})

        # Append using position="end"
        edit_file = workspace_tools["edit_file"]
        result = edit_file.invoke(
            {"path": "test.md", "new_string": "\nLine 2", "position": "end"}
        )

        assert "Appended" in result

        # Verify the change
        content = workspace_manager.read_file("test.md")
        assert content == "Line 1\nLine 2"

    def test_edit_file_position_start_prepends(
        self, workspace_tools, workspace_manager
    ):
        """Test that position='start' prepends to file."""
        # Create a test file
        workspace_manager.write_file("test.md", "Line 2")

        # Read first
        read_file = workspace_tools["read_file"]
        read_file.invoke({"path": "test.md"})

        # Prepend using position="start"
        edit_file = workspace_tools["edit_file"]
        result = edit_file.invoke(
            {"path": "test.md", "new_string": "Line 1\n", "position": "start"}
        )

        assert "Prepended" in result

        # Verify the change
        content = workspace_manager.read_file("test.md")
        assert content == "Line 1\nLine 2"

    def test_edit_file_position_invalid_fails(self, workspace_tools, workspace_manager):
        """Test that invalid position values fail.

        ``position`` is now Literal["start", "end"], so the schema rejects an
        off-vocabulary value before the body runs — the model cannot emit it at
        all. The body's own check remains for callers that skip the schema.
        """
        from pydantic import ValidationError

        # Create a test file
        workspace_manager.write_file("test.md", "Content")

        # Read first
        read_file = workspace_tools["read_file"]
        read_file.invoke({"path": "test.md"})

        edit_file = workspace_tools["edit_file"]

        # Schema layer: rejected up front, file untouched.
        with pytest.raises(ValidationError):
            edit_file.invoke(
                {
                    "path": "test.md",
                    "new_string": "New",
                    "position": "middle",  # Invalid
                }
            )
        assert workspace_manager.read_file("test.md") == "Content"

        # Body layer: still a clean error, never a silent fall-through to
        # replace mode.
        result = edit_file.func(path="test.md", new_string="New", position="middle")
        assert "Error" in result
        assert "Invalid position" in result

    def test_edit_file_replace_requires_old_string(
        self, workspace_tools, workspace_manager
    ):
        """Test that replace mode (no position) requires old_string."""
        # Create a test file
        workspace_manager.write_file("test.md", "Content")

        # Read first
        read_file = workspace_tools["read_file"]
        read_file.invoke({"path": "test.md"})

        # Try replace without old_string
        edit_file = workspace_tools["edit_file"]
        result = edit_file.invoke(
            {
                "path": "test.md",
                "new_string": "New",
                # No old_string, no position
            }
        )

        assert "Error" in result
        assert "old_string is required" in result


class TestWriteFileReadRequirement:
    """Tests for write_file requiring recent read for existing files."""

    def test_write_file_existing_requires_read(
        self, workspace_tools, workspace_manager
    ):
        """Test that overwriting existing file fails without recent read."""
        # Create a test file
        workspace_manager.write_file("test.md", "Original content")

        # Try to overwrite without reading first
        write_file = workspace_tools["write_file"]
        result = write_file.invoke({"path": "test.md", "content": "New content"})

        assert "Error" in result
        assert "read_file" in result.lower()

        # Original content should be unchanged
        content = workspace_manager.read_file("test.md")
        assert content == "Original content"

    def test_write_file_new_no_read_required(self, workspace_tools, workspace_manager):
        """Test that creating a new file doesn't require read."""
        # Write a new file without reading
        write_file = workspace_tools["write_file"]
        result = write_file.invoke(
            {"path": "new_file.md", "content": "Brand new content"}
        )

        assert "Written" in result

        # Verify the file was created
        content = workspace_manager.read_file("new_file.md")
        assert content == "Brand new content"

    def test_write_file_works_after_read(self, workspace_tools, workspace_manager):
        """Test that overwriting works after reading."""
        # Create a test file
        workspace_manager.write_file("test.md", "Original content")

        # Read first
        read_file = workspace_tools["read_file"]
        read_file.invoke({"path": "test.md"})

        # Now overwrite should work
        write_file = workspace_tools["write_file"]
        result = write_file.invoke({"path": "test.md", "content": "New content"})

        assert "Written" in result

        # Verify the change
        content = workspace_manager.read_file("test.md")
        assert content == "New content"

    def test_write_file_requires_fresh_read_after_out_of_band_change(
        self, workspace_tools, workspace_manager, tool_context
    ):
        workspace_manager.write_file("test.md", "Visible line\nAgent observed this")
        workspace_tools["read_file"].invoke(
            {"path": "test.md", "offset": 1, "limit": 1}
        )

        # The content hash is the integrity guard when no live invalidation was
        # delivered. It covers the full text, not only the displayed line range.
        workspace_manager.write_file("test.md", "Visible line\nUser changed this")
        result = workspace_tools["write_file"].invoke(
            {"path": "test.md", "content": "Agent overwrite"}
        )

        assert "read_file" in result.lower()
        assert workspace_manager.read_file("test.md") == (
            "Visible line\nUser changed this"
        )
        assert not tool_context.was_recently_read("test.md")

    def test_rewriting_a_file_the_agent_created_needs_no_read(self, workspace_tools):
        # Bench jobs paid a refused call plus a read_file for every plan.md
        # rewrite: a created file was never recorded as known content.
        write_file = workspace_tools["write_file"]
        assert "Written" in write_file.invoke({"path": "plan.md", "content": "v1"})

        result = write_file.invoke({"path": "plan.md", "content": "v2"})

        assert "Written" in result

    def test_editing_a_file_the_agent_created_needs_no_read(
        self, workspace_tools, workspace_manager
    ):
        workspace_tools["write_file"].invoke(
            {"path": "code/ledger.py", "content": "x = 1\n"}
        )

        result = workspace_tools["edit_file"].invoke(
            {"path": "code/ledger.py", "old_string": "x = 1", "new_string": "x = 2"}
        )

        assert "Edited" in result
        assert workspace_manager.read_file("code/ledger.py") == "x = 2\n"

    def test_consecutive_edits_need_no_read_between(
        self, workspace_tools, workspace_manager
    ):
        workspace_tools["write_file"].invoke({"path": "a.py", "content": "a b c"})
        edit_file = workspace_tools["edit_file"]

        first = edit_file.invoke({"path": "a.py", "old_string": "a", "new_string": "A"})
        second = edit_file.invoke(
            {"path": "a.py", "old_string": "b", "new_string": "B"}
        )

        assert "Edited" in first and "Edited" in second
        assert workspace_manager.read_file("a.py") == "A B c"

    def test_own_write_still_requires_read_after_out_of_band_change(
        self, workspace_tools, workspace_manager, tool_context
    ):
        workspace_tools["write_file"].invoke({"path": "notes.md", "content": "agent"})
        workspace_manager.write_file("notes.md", "user edit")

        result = workspace_tools["write_file"].invoke(
            {"path": "notes.md", "content": "agent again"}
        )

        assert "read_file" in result.lower()
        assert workspace_manager.read_file("notes.md") == "user edit"
        assert not tool_context.was_recently_read("notes.md")


class TestAppendFileRemoved:
    """Tests that append_file is no longer available."""

    def test_append_file_not_in_tools(self, workspace_tools):
        """Test that append_file is not in the returned tools."""
        assert "append_file" not in workspace_tools
