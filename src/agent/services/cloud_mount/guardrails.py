"""Lightweight guardrails for rclone-backed cloud mounts.

This is intentionally a preflight advisory/block, not the full hydration-budget
system described in knowledge-base/knowledge/features/rclone_cloud_mount.md. Runtime transfer
accounting still belongs in a later guard implementation.
"""

from __future__ import annotations

import re
import shlex
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Iterable


_CLOUD_ROOTS = ("/cloud", "/workspace/cloud")
_SHELL_OPERATORS = {"|", "||", "&&", ";", "&"}
_WORKSPACE_CLOUD_ROOT = "cloud"

_WRITE_INDICATORS = re.compile(
    r"(?:>>?|\btee\b|\brm\b|\bmv\b|\bcp\b|\btouch\b|\bmkdir\b|\brmdir\b"
    r"|\brsync\b|\bdd\b|\btruncate\b|\bln\b|\bunzip\b|\btar\b|\bsed\s+(?:-\S*\s+)*-i)"
)


@dataclass(frozen=True)
class CloudScanRisk:
    reason: str


def detect_cloud_scan_risk(command: str) -> CloudScanRisk | None:
    """Return a risk reason for obvious broad cloud-hydration commands.

    The detector is deliberately conservative and argv-oriented. It catches
    common accidental scans while avoiding a brittle free-form regex blocklist.
    """
    try:
        argv = shlex.split(command)
    except ValueError:
        if _mentions_cloud_path(command):
            return CloudScanRisk(
                "command mentions a cloud mount but cannot be parsed safely"
            )
        return None

    if not argv or not _argv_touches_cloud(argv):
        return None

    command_name = _base_command(argv)
    if not command_name:
        return None

    if command_name == "grep" and _has_recursive_flag(argv):
        return CloudScanRisk("recursive grep over a cloud mount can hydrate many files")
    if command_name in {"rg", "ag"}:
        return CloudScanRisk(
            f"{command_name} searches recursively by default when pointed at a cloud mount"
        )
    if command_name in {"du", "tar", "zip", "unzip"}:
        return CloudScanRisk(
            f"{command_name} over a cloud mount can traverse or materialize large trees"
        )
    if command_name in {"rsync", "scp"}:
        return CloudScanRisk(
            f"{command_name} against a cloud mount can copy large remote trees"
        )
    if command_name == "cp" and _has_recursive_flag(argv):
        return CloudScanRisk("recursive copy from a cloud mount can hydrate many files")
    if command_name == "find" and _contains_any(argv, {"-exec", "-execdir"}):
        return CloudScanRisk(
            "find -exec over a cloud mount can run content-reading commands at cloud scale"
        )

    if _contains_any(argv, _SHELL_OPERATORS) and _looks_like_complex_cloud_pipeline(
        argv
    ):
        return CloudScanRisk(
            "complex shell pipeline touching a cloud mount needs a narrower path or review"
        )

    return None


def detect_cloud_delete_risk(command: str) -> CloudScanRisk | None:
    """Flag broad deletes over a cloud mount.

    Over a capture overlay, ``rm -rf`` is O(files) cold backend round-trips and
    can download full file bodies just to whiteout them (design §11.1). Catch
    recursive ``rm`` and ``find ... -delete`` pointed at a cloud path; a single
    ``rm file`` (no -r) is cheap enough to allow.
    """
    try:
        argv = shlex.split(command)
    except ValueError:
        if _mentions_cloud_path(command):
            return CloudScanRisk(
                "delete command mentions a cloud mount but cannot be parsed safely"
            )
        return None
    if not argv or not _argv_touches_cloud(argv):
        return None
    name = _base_command(argv)
    if name == "rm" and _has_recursive_flag(argv):
        return CloudScanRisk(
            "recursive rm over a cloud mount whiteouts each file (O(files) backend round-trips)"
        )
    if name == "find" and _contains_any(argv, {"-delete"}):
        return CloudScanRisk(
            "find -delete over a cloud mount whiteouts matches one by one"
        )
    return None


def format_cloud_delete_guard_message(
    command: str, risk: CloudScanRisk, *, protected: bool
) -> str:
    """The staged-semantics sentence is TRUE only under the capture overlay —
    on a live rw mount a delete is immediate and irreversible. The message must
    never claim safety the session doesn't have (B5 review finding)."""
    if protected:
        semantics = (
            "In protected mode a delete is STAGED (the cloud is untouched until "
            "you apply the diff), but whiteouting each file still costs a "
            "backend round-trip and may download its body first."
        )
    else:
        semantics = (
            "This session's cloud mount is LIVE: a delete removes the real "
            "cloud files immediately and is only recoverable via the cloud's "
            "own version history/trash, if any."
        )
    return (
        "Cloud delete guard: this command was not run because a broad delete "
        "over cloud storage is risky/expensive.\n"
        f"Reason: {risk.reason}.\n"
        f"Command: {command}\n\n"
        f"{semantics} Narrow the path, delete specific files, or confirm with "
        "the operator before a bulk delete."
    )


def command_may_write_cloud(command: str) -> bool:
    """Conservative write-indicator check for the at-quota shell guard.

    Reads never copy-up into the overlay upperdir, so a full-quota upperdir
    must not block them (Slice B deferral: read/write asymmetry). False
    negatives are tolerable — a missed write fails at the FS layer and the
    guard catches the next command.
    """
    return bool(_WRITE_INDICATORS.search(command))


def command_touches_cloud_mount(command: str) -> bool:
    """Return True when an argv-parsable command names a cloud mount path."""
    try:
        argv = shlex.split(command)
    except ValueError:
        return _mentions_cloud_path(command)
    return _argv_touches_cloud(argv)


def workspace_search_touches_cloud(path: str) -> bool:
    """Return True when a workspace search path would include cloud mounts."""
    normalized = _normalize_workspace_path(path)
    return (
        normalized == ""
        or normalized == _WORKSPACE_CLOUD_ROOT
        or normalized.startswith(f"{_WORKSPACE_CLOUD_ROOT}/")
    )


def workspace_path_touches_cloud(path: str) -> bool:
    """Return True when a specific workspace path points into cloud mounts."""
    normalized = _normalize_workspace_path(path)
    return normalized == _WORKSPACE_CLOUD_ROOT or normalized.startswith(
        f"{_WORKSPACE_CLOUD_ROOT}/"
    )


def format_cloud_scan_guard_message(command: str, risk: CloudScanRisk) -> str:
    return (
        "Cloud scan guard: this command was not run because it looks like a "
        "broad operation over rclone-mounted cloud storage.\n"
        f"Reason: {risk.reason}.\n"
        f"Command: {command}\n\n"
        "Use a narrower file or directory under the cloud/ folder, run a metadata-only "
        "query first, or ask the operator before scanning a large cloud tree."
    )


def format_workspace_cloud_search_guard_message(path: str) -> str:
    shown = path or "/"
    return (
        "Cloud scan guard: search_files was not run because the requested "
        f"workspace path ({shown}) includes rclone-mounted cloud storage.\n\n"
        "Use a narrower non-cloud workspace path, inspect a specific cloud "
        "directory with list_files first, or use the cloud search/index path "
        "when it is available."
    )


def _base_command(argv: list[str]) -> str | None:
    for token in argv:
        if token in _SHELL_OPERATORS:
            continue
        return PurePosixPath(token).name
    return None


def _argv_touches_cloud(argv: Iterable[str]) -> bool:
    return any(_is_cloud_path_token(token) for token in argv)


def _mentions_cloud_path(command: str) -> bool:
    return any(root in command for root in _CLOUD_ROOTS)


def _is_cloud_path_token(token: str) -> bool:
    if token in _SHELL_OPERATORS or token.startswith("-"):
        return False
    normalized = token.rstrip("/")
    if normalized in _CLOUD_ROOTS:
        return True
    return any(normalized.startswith(f"{root}/") for root in _CLOUD_ROOTS)


def _has_recursive_flag(argv: Iterable[str]) -> bool:
    for token in argv:
        if token in {"-r", "-R", "--recursive", "--dereference-recursive"}:
            return True
        if token.startswith("-") and not token.startswith("--"):
            if "r" in token[1:] or "R" in token[1:]:
                return True
    return False


def _contains_any(argv: Iterable[str], candidates: set[str]) -> bool:
    return any(token in candidates for token in argv)


def _looks_like_complex_cloud_pipeline(argv: list[str]) -> bool:
    cloud_index = next(
        (index for index, token in enumerate(argv) if _is_cloud_path_token(token)),
        None,
    )
    if cloud_index is None:
        return False
    risky_commands = {"grep", "rg", "ag", "python", "python3", "node", "perl", "ruby"}
    return any(PurePosixPath(token).name in risky_commands for token in argv)


def _normalize_workspace_path(path: str) -> str:
    text = str(path or "").strip()
    if text in {".", "/"}:
        return ""
    if text.startswith("/workspace/"):
        text = text[len("/workspace/") :]
    elif text == "/workspace":
        text = ""
    text = text.strip("/")
    parts = []
    for part in text.split("/"):
        if part in {"", "."}:
            continue
        if part == "..":
            if parts:
                parts.pop()
            continue
        parts.append(part)
    return "/".join(parts)
