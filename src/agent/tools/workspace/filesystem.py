"""Filesystem operations tools for the Universal Agent.

Provides filesystem operations within the workspace including:
- Directory listing and navigation
- File deletion, moving, copying, renaming
- File search and existence checks
- Workspace summary and document info
"""

import logging
from typing import Any, List

from langchain_core.tools import tool
from shared.runtime.core.workspace_backend import (
    SEARCH_RESULT_HARD_CAP,
    WorkspaceUnavailableError,
)

from agent.tools.context import ToolContext
from agent.services.cloud_mount.guardrails import (
    format_workspace_cloud_search_guard_message,
    workspace_search_touches_cloud,
)
from agent.utils.pdf import PDFReader, format_document_info

from shared.tool_catalog.definitions import (
    FILESYSTEM_TOOLS_METADATA as FILESYSTEM_TOOLS_METADATA,
)

logger = logging.getLogger(__name__)

# File purpose heuristics for workspace_summary.md
FILE_PURPOSE_MAP = {
    "plan.md": "Execution plan",
    "instructions.md": "Task instructions",
    "workspace_summary.md": "Workspace state snapshot",
    "research.md": "Research findings",
    "document_analysis.md": "Document analysis",
    "extraction_results.md": "Extraction results",
}


def _infer_file_purpose(filename: str) -> str:
    """Infer the purpose of a file from its name.

    Args:
        filename: Name of the file

    Returns:
        Inferred purpose string
    """
    # Check direct mapping
    if filename in FILE_PURPOSE_MAP:
        return FILE_PURPOSE_MAP[filename]

    # Pattern-based inference
    lower = filename.lower()

    if "chunk" in lower:
        return "Document chunk"
    if "candidate" in lower:
        return "Requirement candidate"
    if "requirement" in lower:
        return "Requirements"
    if "output" in lower or "result" in lower:
        return "Output data"
    if "config" in lower:
        return "Configuration"
    if "log" in lower:
        return "Log file"
    if lower.endswith(".pdf"):
        return "Source document"
    if lower.endswith(".json"):
        return "Structured data"
    if lower.endswith(".md"):
        return "Documentation"

    return "Working file"


# Operators of the regex dialects models reach for ("foo|bar", "\bword\b",
# "a.*b", "(x|y)+") that search_files matches as plain text. A dot is left out:
# it is far more common in literal queries ("config.yaml") than as a wildcard.
_REGEX_SHAPED_CHARS = frozenset("|*+?()[]{}^$\\")


def _literal_query_note(query: str, has_shell: bool) -> str:
    """Why a regex-shaped query found nothing, and what to do instead.

    Offers the shell only when a shell TOOL is bound (not merely a
    shell-capable backend), so neither a virtual workspace nor an expert
    granted ``shell: []`` is told to use a capability it lacks.
    knowledge-base/knowledge/issues/search_files_literal_query_contract.md
    """
    if not any(ch in _REGEX_SHAPED_CHARS for ch in query):
        return ""
    alternative = (
        "For a regular expression, run rg or grep -E with your shell tool."
        if has_shell
        else "To find any of several words, search for each one separately."
    )
    return (
        "\n(The query was matched literally, as plain text: characters such as "
        f"| * ( ) [ ] ^ $ and backslash are not regex operators here. {alternative})"
    )


# Tool metadata for registry
# Phase availability: filesystem tools are available in both strategic and tactical modes


def create_filesystem_tools(context: ToolContext) -> List[Any]:
    """Create filesystem operation tools with injected context.

    Args:
        context: ToolContext with workspace_manager

    Returns:
        List of LangChain tool functions

    Raises:
        ValueError: If context doesn't have a workspace_manager
    """
    if not context.has_workspace():
        raise ValueError(
            "ToolContext must have a workspace_manager for filesystem tools"
        )

    workspace = context.workspace_manager

    # Get config values
    max_read_words = context.get_config("max_read_words")
    if max_read_words is None:
        max_read_size_legacy = context.get_config("max_read_size", 137_500)
        max_read_words = int(max_read_size_legacy / 5.5)

    max_search_results = context.get_config("max_search_results", 50)

    # Average bytes per word (for estimation)
    BYTES_PER_WORD = 5.5

    # Initialize PDF reader for document info
    pdf_reader = PDFReader(max_words_per_read=max_read_words)

    @tool
    def list_files(path: str = "", pattern: str = "*", depth: int = 1) -> str:
        """List files and directories in a workspace path with optional depth.

        Directories are shown with a trailing slash. By default shows one level
        of subdirectory contents to help navigate the workspace structure.

        Args:
            path: Relative directory path (empty for workspace root)
            pattern: Glob pattern to filter files (e.g., "*.md", "*.json")
            depth: How many levels deep to show (0=flat, 1=show subdir contents,
                   max 3). Default is 1.

        Returns:
            List of files and directories, or message if empty

        Example output (depth=1):
            Contents of /:

            Directories:
              archive/
                phase_1_strategic/
              documents/
                report.pdf

            Files:
              instructions.md
              plan.md
        """
        # Cap depth at 3 to prevent excessive output
        depth = max(0, min(3, depth))

        def _strip_prefix(item_path: str, base_path: str) -> str:
            """Strip parent directory prefix from workspace-relative path.

            workspace.list_files() returns paths relative to the workspace root
            (e.g., "output/data/"), but for display we want paths relative to
            the queried directory (e.g., "data/").
            """
            if not base_path:
                return item_path
            prefix = base_path.rstrip("/") + "/"
            if item_path.startswith(prefix):
                return item_path[len(prefix) :]
            return item_path

        def list_recursive(dir_path: str, current_depth: int, indent: str) -> list[str]:
            """Recursively list directory contents up to specified depth."""
            lines = []
            try:
                items = workspace.list_files(dir_path, pattern=pattern)
            except Exception:
                return lines

            dirs = sorted([i for i in items if i.endswith("/")])
            files = sorted([i for i in items if not i.endswith("/")])

            # Add files at this level (strip prefix for display)
            for f in files:
                lines.append(f"{indent}{_strip_prefix(f, dir_path)}")

            # Add directories and optionally their contents
            for d in dirs:
                lines.append(f"{indent}{_strip_prefix(d, dir_path)}")
                if current_depth < depth:
                    # d is already workspace-relative, use directly
                    sub_lines = list_recursive(
                        d.rstrip("/"), current_depth + 1, indent + "  "
                    )
                    lines.extend(sub_lines)

            return lines

        try:
            items = workspace.list_files(path, pattern=pattern)

            if not items:
                return f"No files found in: {path or '/'}" + (
                    f" matching '{pattern}'" if pattern != "*" else ""
                )

            # Format output header
            header = f"Contents of {path or '/'}:"
            if pattern != "*":
                header += f" (pattern: {pattern})"

            # Separate directories and files at root level
            dirs = sorted([i for i in items if i.endswith("/")])
            files = sorted([i for i in items if not i.endswith("/")])

            lines = [header, ""]

            if dirs:
                lines.append("Directories:")
                for d in dirs:
                    lines.append(f"  {_strip_prefix(d, path)}")
                    if depth > 0:
                        # d is already workspace-relative, use directly
                        sub_lines = list_recursive(d.rstrip("/"), 1, "    ")
                        lines.extend(sub_lines)

            if files:
                if dirs:
                    lines.append("")
                lines.append("Files:")
                for f in files:
                    lines.append(f"  {_strip_prefix(f, path)}")

            return "\n".join(lines)

        except ValueError as e:
            return f"Error: {str(e)}"
        except WorkspaceUnavailableError:
            raise
        except Exception as e:
            logger.error(f"list_files error for {path}: {e}")
            return f"Error listing files: {str(e)}"

    @tool
    def delete_file(path: str) -> str:
        """Delete a file or empty directory from the workspace.

        Cannot delete non-empty directories (delete contents first).

        Args:
            path: Relative path to delete

        Returns:
            Confirmation or error message
        """
        try:
            success = workspace.delete_file(path)

            if success:
                return f"Deleted: {path}"
            else:
                return f"Not found: {path}"

        except ValueError as e:
            return f"Error: {str(e)}"
        except WorkspaceUnavailableError:
            raise
        except Exception as e:
            logger.error(f"delete_file error for {path}: {e}")
            return f"Error deleting: {str(e)}"

    @tool
    def search_files(
        query: str,
        path: str = "",
        case_sensitive: bool = False,
        exclude_dirs: list[str] | None = None,
    ) -> str:
        """Find lines that contain a literal piece of text in workspace files.

        The query is plain text, not a regex or glob: every character is
        matched as written, so "foo|bar" finds only lines containing the exact
        text "foo|bar" (not lines with foo or bar), and characters such as
        . * ( ) [ ] ^ $ and backslash have no special meaning. To find any of
        several words, search once per word. For a regular expression, use
        rg or grep -E through a shell tool if one is available to you.

        Args:
            query: Literal text to find within a single line, e.g.
                "def search_files" or "TODO(auth)"
            path: Workspace-relative directory or single file to search
                (empty for the entire workspace)
            case_sensitive: Match letter case exactly (default: case-insensitive)
            exclude_dirs: Directory names to skip at any depth, e.g.
                ["node_modules", ".git"] (glob patterns allowed)

        Returns:
            Matching lines grouped by file, with line numbers. Binary files
            (PDF, Word, images, archives) are not searched.
        """
        if not query:
            return "Error: query is empty — pass the literal text to find."
        if "\n" in query or "\r" in query:
            return (
                "Error: query spans several lines, but matching happens within "
                "one line. Search for a single line of it."
            )
        try:
            cloud_mount_cfg = context.get_config("cloud_mount", {})
            if (
                isinstance(cloud_mount_cfg, dict)
                and cloud_mount_cfg.get("active")
                and workspace_search_touches_cloud(path)
            ):
                return format_workspace_cloud_search_guard_message(path)

            results = workspace.search_files(
                query,
                path=path,
                case_sensitive=case_sensitive,
                exclude_dirs=exclude_dirs,
            )

            if not results:
                # An empty result must not read as "the text is absent" when
                # the search itself was off: a path that does not exist, or a
                # regex-shaped query that was (by contract) matched literally.
                if path and not workspace.exists(path):
                    return f"Error: path not found: {path}"
                return f"No matches found for: {query}" + _literal_query_note(
                    query, context.has_shell_tool()
                )

            # Limit results
            total = len(results)
            results = results[:max_search_results]

            lines = [f"Search results for '{query}':", ""]

            # Group by file
            current_file = None
            for result in results:
                file_path = result["path"]
                if file_path != current_file:
                    if current_file is not None:
                        lines.append("")
                    lines.append(f"  {file_path}:")
                    current_file = file_path

                line_num = result.get("line_number", "?")
                line_text = result.get("line", "").strip()
                # Truncate long lines
                if len(line_text) > 100:
                    line_text = line_text[:100] + "..."
                lines.append(f"    L{line_num}: {line_text}")

            if total == SEARCH_RESULT_HARD_CAP:
                lines.append("")
                lines.append(
                    f"[Showing {max_search_results} of {SEARCH_RESULT_HARD_CAP}+ "
                    f"matches (server-side capped)]"
                )
            elif total > max_search_results:
                lines.append("")
                lines.append(f"[Showing {max_search_results} of {total} matches]")

            return "\n".join(lines)

        except ValueError as e:
            return f"Error: {str(e)}"
        except WorkspaceUnavailableError:
            raise
        except Exception as e:
            logger.error(f"search_files error: {e}")
            return f"Error searching: {str(e)}"

    @tool
    def file_exists(path: str) -> str:
        """Check if a file or directory exists in the workspace.

        Args:
            path: Relative path to check

        Returns:
            "exists" or "not found" message
        """
        try:
            if workspace.exists(path):
                full_path = workspace.get_path(path)
                if full_path.is_dir():
                    return f"Exists (directory): {path}"
                else:
                    return f"Exists (file): {path}"
            else:
                return f"Not found: {path}"

        except ValueError as e:
            return f"Error: {str(e)}"
        except WorkspaceUnavailableError:
            raise
        except Exception as e:
            logger.error(f"file_exists error for {path}: {e}")
            return f"Error: {str(e)}"

    @tool
    def move_file(source: str, dest: str) -> str:
        """Move or rename a file/directory in the workspace.

        Creates parent directories for destination if they don't exist.
        Works for both files and directories.

        Args:
            source: Source path relative to workspace root
            dest: Destination path relative to workspace root

        Returns:
            Confirmation message with source and destination paths
        """
        try:
            workspace.move_file(source, dest)
            return f"Moved: {source} -> {dest}"

        except FileNotFoundError:
            return f"Error: Source not found: {source}"
        except ValueError as e:
            return f"Error: {str(e)}"
        except WorkspaceUnavailableError:
            raise
        except Exception as e:
            logger.error(f"move_file error for {source} -> {dest}: {e}")
            return f"Error moving file: {str(e)}"

    @tool
    def rename_file(path: str, new_name: str) -> str:
        """Rename a file or directory (keeps it in the same location).

        Simpler interface when you just want to change the name without
        moving to a different directory.

        Example: rename_file("documents/report.md", "final_report.md")
        Result: documents/report.md -> documents/final_report.md

        Args:
            path: Current path relative to workspace root
            new_name: New filename (not a path, just the name)

        Returns:
            Confirmation message with old and new paths
        """
        try:
            # Extract directory from original path and combine with new name
            from pathlib import Path as PurePath

            original = PurePath(path)
            new_path = (
                str(original.parent / new_name)
                if original.parent != PurePath(".")
                else new_name
            )

            workspace.move_file(path, new_path)
            return f"Renamed: {path} -> {new_path}"

        except FileNotFoundError:
            return f"Error: File not found: {path}"
        except ValueError as e:
            return f"Error: {str(e)}"
        except WorkspaceUnavailableError:
            raise
        except Exception as e:
            logger.error(f"rename_file error for {path} -> {new_name}: {e}")
            return f"Error renaming file: {str(e)}"

    @tool
    def copy_file(source: str, dest: str) -> str:
        """Copy a file within the workspace.

        Creates parent directories for destination if they don't exist.
        Only works for files, not directories.

        Args:
            source: Source file path relative to workspace root
            dest: Destination path relative to workspace root

        Returns:
            Confirmation message with source and destination paths
        """
        try:
            workspace.copy_file(source, dest)
            return f"Copied: {source} -> {dest}"

        except FileNotFoundError:
            return f"Error: Source not found: {source}"
        except ValueError as e:
            return f"Error: {str(e)}"
        except WorkspaceUnavailableError:
            raise
        except Exception as e:
            logger.error(f"copy_file error for {source} -> {dest}: {e}")
            return f"Error copying file: {str(e)}"

    @tool
    def get_document_info(path: str) -> str:
        """Get metadata about a document without reading its full content.

        Returns page count, estimated size, and available metadata.
        Use this before reading large PDFs to plan page-by-page access.

        Args:
            path: Relative path to the document (e.g., "documents/GoBD.pdf")

        Returns:
            Document metadata including page count, estimated tokens, file size.
            For non-PDF files, returns basic file information.
        """
        try:
            if not workspace.exists(path):
                return f"Error: File not found: {path}"

            full_path = workspace.get_path(path)
            if full_path.is_dir():
                return f"Error: '{path}' is a directory, not a file."

            # For PDF files, get detailed info
            if full_path.suffix.lower() == ".pdf":
                if not pdf_reader.is_available():
                    return "Error: PDF info requires pdfplumber. Install with: pip install pdfplumber"

                try:
                    # Materialize the file on the local filesystem first —
                    # required for remote workspace backends where get_path()
                    # returns a remote-only path that local PDF I/O cannot open.
                    with workspace.local_copy(path) as local_path:
                        info = pdf_reader.get_document_info(local_path)

                    # local_copy() yields a temp file — restore the
                    # caller-facing name so the display shows the real path.
                    info["file_name"] = path.rsplit("/", 1)[-1]

                    # Format with reading suggestions
                    lines = [format_document_info(info), ""]

                    # Add reading suggestions based on size
                    total_chars = info.get("estimated_total_chars", 0)
                    page_count = info.get("page_count", 0)

                    # Convert chars to estimated words (conservative estimate for PDF text)
                    estimated_words = total_chars // 5

                    if estimated_words <= max_read_words:
                        lines.append("This document fits in a single read.")
                        lines.append(f'Use: read_file("{path}")')
                    else:
                        # Estimate how many pages fit in one read
                        chars_per_page = info.get("estimated_chars_per_page", 3000)
                        words_per_page = (
                            chars_per_page // 5
                        )  # Approximate words per page
                        pages_per_read = (
                            max(1, max_read_words // words_per_page)
                            if words_per_page > 0
                            else 1
                        )

                        lines.append(
                            f"Suggested approach (document exceeds single-read limit of ~{max_read_words:,} words):"
                        )
                        lines.append(f"- Read ~{pages_per_read} pages at a time")
                        lines.append(
                            f'- Start with: read_file("{path}", page_start=1, page_end={min(pages_per_read, page_count)})'
                        )
                        if page_count > pages_per_read:
                            lines.append(
                                f'- Continue with: read_file("{path}", page_start={pages_per_read + 1}, page_end={min(pages_per_read * 2, page_count)})'
                            )

                    return "\n".join(lines)

                except WorkspaceUnavailableError:
                    raise
                except Exception as e:
                    logger.error(f"PDF info error for {path}: {e}")
                    return f"Error getting PDF info: {str(e)}"

            # For non-PDF files, return basic info
            file_size = workspace.get_size(path)
            estimated_words = int(file_size / BYTES_PER_WORD)
            estimated_tokens = estimated_words  # Roughly 1 word ≈ 1 token

            lines = [
                f"Document: {full_path.name}",
                f"Type: {full_path.suffix or 'unknown'}",
                f"File size: {file_size:,} bytes (~{estimated_words:,} words, ~{estimated_tokens:,} tokens)",
            ]

            if estimated_words <= max_read_words:
                lines.append("\nThis file fits in a single read.")
                lines.append(f'Use: read_file("{path}")')
            else:
                lines.append(
                    f"\nFile exceeds single-read limit (~{max_read_words:,} words)."
                )
                lines.append("Consider using search_files() or chunking.")

            return "\n".join(lines)

        except ValueError as e:
            return f"Error: {str(e)}"
        except WorkspaceUnavailableError:
            raise
        except Exception as e:
            logger.error(f"get_document_info error for {path}: {e}")
            return f"Error: {str(e)}"

    @tool
    def create_directory(path: str) -> str:
        """Create a directory (and any parent directories) in the workspace.

        Use this to explicitly create directory structures before writing files.

        Args:
            path: Relative path of the directory to create

        Returns:
            Confirmation or error message
        """
        try:
            workspace.create_directory(path)
            return f"Created directory: {path}"

        except ValueError as e:
            return f"Error: {str(e)}"
        except WorkspaceUnavailableError:
            raise
        except Exception as e:
            logger.error(f"create_directory error for {path}: {e}")
            return f"Error creating directory: {str(e)}"

    @tool
    def delete_directory(path: str) -> str:
        """Delete a directory and all its contents from the workspace.

        WARNING: This recursively deletes everything inside the directory.
        Cannot delete the workspace root.

        Args:
            path: Relative path of the directory to delete

        Returns:
            Confirmation or error message
        """
        try:
            success = workspace.delete_directory(path)

            if success:
                return f"Deleted directory and all contents: {path}"
            else:
                return f"Not found: {path}"

        except ValueError as e:
            return f"Error: {str(e)}"
        except WorkspaceUnavailableError:
            raise
        except Exception as e:
            logger.error(f"delete_directory error for {path}: {e}")
            return f"Error deleting directory: {str(e)}"

    return [
        list_files,
        delete_file,
        search_files,
        file_exists,
        move_file,
        rename_file,
        copy_file,
        get_document_info,
        create_directory,
        delete_directory,
    ]
