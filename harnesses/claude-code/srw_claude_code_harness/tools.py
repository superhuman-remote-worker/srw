"""The four Claude-Code-shaped tools (Bash, Read, Write, Edit) over the SSH workspace.

Names, parameters and result texts copy Claude Code's so the model's habits carry
over. Handlers never raise: every failure becomes an ``is_error`` result.
"""

from __future__ import annotations

import asyncio
import posixpath
import shlex
from collections.abc import Sequence
from typing import Any

from .workspace import Workspace, WorkspaceError, _failure

TOOL_NAMES = ("Bash", "Read", "Write", "Edit")
OUTPUT_LIMIT_CHARS = 30_000
READ_DEFAULT_LIMIT = 2000
READ_MAX_LINE_CHARS = 2000
BASH_DEFAULT_TIMEOUT_MS = 120_000
BASH_MAX_TIMEOUT_MS = 600_000
SAVED_OUTPUT_DIR = ".srw/tool-output"
READ_FETCH_MAX_BYTES = 4 * 1024 * 1024
EDIT_MAX_BYTES = 10 * 1024 * 1024

_PATH_MARK = "<path>"


def _ok(text: str) -> dict[str, Any]:
    return {"content": [{"type": "text", "text": text}]}


def _err(text: str) -> dict[str, Any]:
    return {"content": [{"type": "text", "text": text}], "is_error": True}


def _truncate(text: str, limit: int, saved: str | None) -> tuple[str, bool]:
    if len(text) <= limit:
        return text, False
    head = limit * 2 // 3
    tail = limit - head
    note = f"; full output saved to {saved}" if saved else ""
    omitted = len(text) - head - tail
    middle = f"\n… [truncated {omitted} characters{note}] …\n"
    return text[:head] + middle + text[len(text) - tail :], True


def truncate_output(text: str, *, limit: int = OUTPUT_LIMIT_CHARS) -> tuple[str, bool]:
    """Keep the first 2/3 and last 1/3 of ``limit``; the caller fills in ``<path>``."""
    return _truncate(text, limit, _PATH_MARK)


def _too_large(size: int | None) -> dict[str, Any]:
    known = f"{size} bytes" if size is not None else f"over {EDIT_MAX_BYTES} bytes"
    return _err(f"File is too large to edit ({known}; limit {EDIT_MAX_BYTES})")


def _int(value: Any, default: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return default
    return int(value)


class ToolHandlers:
    def __init__(self, workspace: Workspace) -> None:
        self.workspace = workspace
        self._bash_lock = asyncio.Lock()
        self._saved = 0
        # Resolved absolute workspace paths that Write or Edit changed.
        self.written_paths: set[str] = set()

    async def bash(self, args: dict) -> dict:
        try:
            command = args.get("command")
            if not isinstance(command, str) or not command.strip():
                return _err("command is required")
            ms = _int(args.get("timeout"), BASH_DEFAULT_TIMEOUT_MS)
            if ms <= 0:
                ms = BASH_DEFAULT_TIMEOUT_MS
            ms = min(ms, BASH_MAX_TIMEOUT_MS)
            async with self._bash_lock:
                spill = posixpath.join(
                    self.workspace.root, SAVED_OUTPUT_DIR, f"{self._saved + 1}.txt"
                )
                result = await self.workspace.run(
                    command,
                    timeout=ms / 1000,
                    spill_path=self.workspace.resolve(spill),
                    spill_threshold=OUTPUT_LIMIT_CHARS,
                )
                if result.spilled_to:
                    self._saved += 1
            text = result.stdout
            if result.stderr:
                text += ("\n" if text else "") + result.stderr
            if not result.timed_out and result.exit_code != 0:
                text += f"\nExit code {result.exit_code}"
            text, _ = _truncate(text, OUTPUT_LIMIT_CHARS, result.spilled_to)
            if result.timed_out:
                head = f"Command timed out after {ms / 1000:g} seconds"
                return _err(f"{text}\n\n{head}" if text else head)
            return _ok(text)
        except WorkspaceError as exc:
            return _err(str(exc))
        except Exception as exc:  # noqa: BLE001 - handlers must not raise
            return _err(f"Bash failed: {type(exc).__name__}: {exc}"[:300])

    async def _stat(self, resolved: str) -> tuple[bool, int, dict | None]:
        """Remote regular-file check; returns (ok, size, error_result)."""
        found = await self.workspace.stat(resolved)
        if found is None:
            return False, 0, _err("File does not exist.")
        kind, size = found
        if not kind.startswith("regular"):
            return False, 0, _err(f"{resolved} is not a regular file")
        return True, size, None

    async def read(self, args: dict) -> dict:
        try:
            path = args.get("file_path")
            if not isinstance(path, str) or not path:
                return _err("file_path is required")
            resolved = self.workspace.resolve(path)
            ok, size, error = await self._stat(resolved)
            if not ok:
                return error or _err("File does not exist.")
            if size == 0:
                return _ok("(empty file)")
            if await self.workspace.is_binary(resolved):
                return _err("Cannot read a binary file.")
            offset = max(1, _int(args.get("offset"), 1))
            limit = max(1, _int(args.get("limit"), READ_DEFAULT_LIMIT))
            end = offset + limit - 1
            rc, data, stderr = await self.workspace._exec(
                f"sed -n '{offset},{end}p;{end}q' -- {shlex.quote(resolved)}"
                f" | head -c {READ_FETCH_MAX_BYTES}",
                stdout_limit=READ_FETCH_MAX_BYTES,
            )
            if rc != 0:
                return _err(_failure(rc, stderr))
            lines = data.decode("utf-8", errors="replace").split("\n")
            if lines and lines[-1] == "":
                lines.pop()
            if not lines:
                return _ok(f"(no lines at offset {offset})")
            out: list[str] = []
            used = 0
            for number, line in enumerate(lines, start=offset):
                if len(line) > READ_MAX_LINE_CHARS:
                    line = line[:READ_MAX_LINE_CHARS] + "…"
                rendered = f"{number:>6}\t{line}"
                if out and used + len(rendered) + 1 > OUTPUT_LIMIT_CHARS:
                    out.append(
                        f"… [output capped at line {number - 1}; "
                        f"continue with offset={number}]"
                    )
                    break
                out.append(rendered)
                used += len(rendered) + 1
            return _ok("\n".join(out))
        except WorkspaceError as exc:
            return _err(str(exc))
        except Exception as exc:  # noqa: BLE001
            return _err(f"Read failed: {type(exc).__name__}: {exc}"[:300])

    async def write(self, args: dict) -> dict:
        try:
            path, content = args.get("file_path"), args.get("content")
            if not isinstance(path, str) or not path:
                return _err("file_path is required")
            if not isinstance(content, str):
                return _err("content is required")
            resolved = self.workspace.resolve(path)
            data = content.encode("utf-8")
            # An existing target that is not a regular file is refused remotely
            # ("<path> is not a regular file"), so a FIFO cannot block the write.
            await self.workspace.write_bytes(resolved, data)
            self.written_paths.add(resolved)
            return _ok(f"Wrote {len(data)} bytes to {resolved}")
        except WorkspaceError as exc:
            return _err(str(exc))
        except Exception as exc:  # noqa: BLE001
            return _err(f"Write failed: {type(exc).__name__}: {exc}"[:300])

    async def edit(self, args: dict) -> dict:
        try:
            old, new = args.get("old_string"), args.get("new_string")
            if not isinstance(old, str) or not isinstance(new, str):
                return _err("old_string and new_string are required")
            if old == new:
                return _err("old_string and new_string are identical; nothing to do")
            if not old:
                return _err("old_string must not be empty")
            path = args.get("file_path")
            if not isinstance(path, str) or not path:
                return _err("file_path is required")
            resolved = self.workspace.resolve(path)
            ok, size, error = await self._stat(resolved)
            if not ok:
                return error or _err("File does not exist.")
            if size > EDIT_MAX_BYTES:
                return _too_large(size)
            if await self.workspace.is_binary(resolved):
                return _err("Cannot edit a binary file.")
            # Bounded: the file may have grown since the stat.
            data = await self.workspace.read_bytes(resolved, limit=EDIT_MAX_BYTES + 1)
            if len(data) > EDIT_MAX_BYTES:
                return _too_large(None)
            try:
                text = data.decode("utf-8")
            except UnicodeDecodeError:
                return _err("Cannot edit a file that is not valid UTF-8.")
            count = text.count(old)
            if count == 0:
                return _err("old_string not found in the file")
            replace_all = args.get("replace_all") is True
            if count > 1 and not replace_all:
                return _err(
                    f"old_string occurs {count} times; add context to make it "
                    "unique or set replace_all"
                )
            text = text.replace(old, new) if replace_all else text.replace(old, new, 1)
            await self.workspace.write_bytes(resolved, text.encode("utf-8"))
            self.written_paths.add(resolved)
            n = count if replace_all else 1
            return _ok(f"Replaced {n} occurrence(s) in {resolved}")
        except WorkspaceError as exc:
            return _err(str(exc))
        except Exception as exc:  # noqa: BLE001
            return _err(f"Edit failed: {type(exc).__name__}: {exc}"[:300])


_SCHEMAS: dict[str, tuple[str, dict[str, Any]]] = {
    "Bash": (
        "Run a bash command in the workspace. The working directory persists "
        "between calls; shell variables do not. Output over 30000 characters is "
        "truncated and the full text saved to a file you can Read.",
        {
            "type": "object",
            "properties": {
                "command": {"type": "string", "description": "The command to run"},
                "timeout": {
                    "type": "integer",
                    "description": "Timeout in milliseconds (default 120000, max 600000)",
                },
            },
            "required": ["command"],
        },
    ),
    "Read": (
        "Read a text file from the workspace with line numbers. Use offset "
        "(1-based line) and limit to page through large files.",
        {
            "type": "object",
            "properties": {
                "file_path": {"type": "string", "description": "Path of the file"},
                "offset": {"type": "integer", "description": "First line, 1-based"},
                "limit": {"type": "integer", "description": "Number of lines"},
            },
            "required": ["file_path"],
        },
    ),
    "Write": (
        "Write a file in the workspace, replacing it if it exists. Parent "
        "directories are created.",
        {
            "type": "object",
            "properties": {
                "file_path": {"type": "string", "description": "Path of the file"},
                "content": {"type": "string", "description": "Full file content"},
            },
            "required": ["file_path", "content"],
        },
    ),
    "Edit": (
        "Replace an exact string in a file. Fails if old_string is missing or "
        "occurs more than once, unless replace_all is true.",
        {
            "type": "object",
            "properties": {
                "file_path": {"type": "string", "description": "Path of the file"},
                "old_string": {"type": "string", "description": "Text to replace"},
                "new_string": {"type": "string", "description": "Replacement text"},
                "replace_all": {
                    "type": "boolean",
                    "description": "Replace every occurrence",
                },
            },
            "required": ["file_path", "old_string", "new_string"],
        },
    ),
}


def make_tools(
    workspace: Workspace,
    names: Sequence[str],
    handlers: ToolHandlers | None = None,
) -> list:
    from claude_agent_sdk import tool

    annotations = None
    try:
        from mcp.types import ToolAnnotations

        annotations = ToolAnnotations(maxResultSizeChars=OUTPUT_LIMIT_CHARS + 1024)
    except Exception:  # noqa: BLE001 - optional hint only
        annotations = None

    if handlers is None:
        handlers = ToolHandlers(workspace)
    methods = {
        "Bash": handlers.bash,
        "Read": handlers.read,
        "Write": handlers.write,
        "Edit": handlers.edit,
    }
    tools = []
    for name in names:
        if name not in methods:
            raise ValueError(f"Unknown tool: {name}")
        description, schema = _SCHEMAS[name]
        tools.append(tool(name, description, schema, annotations)(methods[name]))
    return tools
