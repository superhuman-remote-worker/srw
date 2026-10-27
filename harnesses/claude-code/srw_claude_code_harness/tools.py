"""The four Claude-Code-shaped tools (Bash, Read, Write, Edit) over the SSH workspace.

Names, parameters and result texts copy Claude Code's so the model's habits carry
over. Handlers never raise: every failure becomes an ``is_error`` result.
"""

from __future__ import annotations

import asyncio
import posixpath
from collections.abc import Sequence
from typing import Any

from .workspace import Workspace, WorkspaceError

OUTPUT_LIMIT_CHARS = 30_000
READ_DEFAULT_LIMIT = 2000
READ_MAX_LINE_CHARS = 2000
BASH_DEFAULT_TIMEOUT_MS = 120_000
BASH_MAX_TIMEOUT_MS = 600_000
SAVED_OUTPUT_DIR = ".srw/tool-output"

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


def _int(value: Any, default: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return default
    return int(value)


class ToolHandlers:
    def __init__(self, workspace: Workspace) -> None:
        self.workspace = workspace
        self._bash_lock = asyncio.Lock()
        self._saved = 0

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
                spill = self.workspace.resolve(
                    f"{SAVED_OUTPUT_DIR}/{self._saved + 1}.txt"
                )
                result = await self.workspace.run(
                    command,
                    timeout=ms / 1000,
                    spill_path=spill,
                    spill_threshold=OUTPUT_LIMIT_CHARS,
                )
                if result.spilled_to:
                    self._saved += 1
            text = result.stdout
            if result.stderr:
                text += ("\n" if text else "") + result.stderr
            if result.timed_out:
                seconds = ms / 1000
                head = f"Command timed out after {seconds:g} seconds"
                return _err(f"{text}\n\n{head}" if text else head)
            if result.exit_code != 0:
                text += f"\nExit code {result.exit_code}"
            saved = None
            if result.spilled_to:
                saved = posixpath.relpath(result.spilled_to, self.workspace.root)
            text, _ = _truncate(text, OUTPUT_LIMIT_CHARS, saved)
            return _ok(text)
        except WorkspaceError as exc:
            return _err(str(exc))
        except Exception as exc:  # noqa: BLE001 - handlers must not raise
            return _err(f"Bash failed: {type(exc).__name__}: {exc}"[:300])

    async def _load(self, path: Any) -> tuple[str | None, bytes | None, dict | None]:
        """Resolve and fetch a text file; returns (resolved, data, error_result)."""
        if not isinstance(path, str) or not path:
            return None, None, _err("file_path is required")
        resolved = self.workspace.resolve(path)
        if not await self.workspace.exists(resolved):
            return resolved, None, _err("File does not exist.")
        data = await self.workspace.read_bytes(resolved)
        if b"\x00" in data[:8192]:
            return resolved, None, _err("Cannot process a binary file.")
        return resolved, data, None

    async def read(self, args: dict) -> dict:
        try:
            _, data, error = await self._load(args.get("file_path"))
            if error or data is None:
                return error or _err("File does not exist.")
            if not data:
                return _ok("(empty file)")
            offset = max(1, _int(args.get("offset"), 1))
            limit = max(1, _int(args.get("limit"), READ_DEFAULT_LIMIT))
            lines = data.decode("utf-8", errors="replace").splitlines()
            chosen = lines[offset - 1 : offset - 1 + limit]
            if not chosen:
                return _ok(f"(no lines at offset {offset}; file has {len(lines)})")
            out = []
            for number, line in enumerate(chosen, start=offset):
                if len(line) > READ_MAX_LINE_CHARS:
                    line = line[:READ_MAX_LINE_CHARS] + "…"
                out.append(f"{number:>6}\t{line}")
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
            await self.workspace.write_bytes(resolved, data)
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
            resolved, data, error = await self._load(args.get("file_path"))
            if error or data is None or resolved is None:
                return error or _err("File does not exist.")
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


def make_tools(workspace: Workspace, names: Sequence[str]) -> list:
    from claude_agent_sdk import tool

    annotations = None
    try:
        from mcp.types import ToolAnnotations

        annotations = ToolAnnotations(maxResultSizeChars=OUTPUT_LIMIT_CHARS + 1024)
    except Exception:  # noqa: BLE001 - optional hint only
        annotations = None

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
