"""The texts of an expert's live voice entry, read the same way on both sides.

A roster entry with ``driver: live`` names its voice in a ``live`` block:
``persona`` and ``facts`` as prose, or ``persona_file`` / ``facts_file`` beside
the expert that names them. The orchestrator builds the session's instructions
from them (``services/live/instructions.py``); the job's back office is told
the facts when its voice subagent attaches (``agent/subagents/live.py``).

Design: knowledge-base/knowledge/features/live_session_jobs.md §6.3–6.4.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from shared.runtime.core.loader import get_project_root


class LiveVoiceError(ValueError):
    """The expert's live voice entry cannot be used (a contradiction)."""


def _read_entry_file(entry: Mapping[str, Any], name: str, key: str) -> str:
    """A ``persona_file``/``facts_file`` beside the expert that names it."""
    directory = entry.get("_deployment_dir")
    if not isinstance(name, str) or not name or not directory:
        raise LiveVoiceError(
            f"live.{key} names a file this expert has no directory for"
        )
    relative = Path(name)
    if relative.is_absolute() or ".." in relative.parts:
        raise LiveVoiceError(
            f"live.{key} must be a file name in the expert's directory"
        )
    base = Path(directory)
    if not base.is_absolute():
        base = get_project_root() / base
    path = (base / relative).resolve()
    if not path.is_relative_to(base.resolve()) or not path.is_file():
        raise LiveVoiceError(f"live.{key} names no file in the expert's directory")
    return path.read_text(encoding="utf-8")


def live_entry_text(live: Mapping[str, Any], entry: Mapping[str, Any], key: str) -> str:
    """``live.<key>`` or the file ``live.<key>_file`` names; ``""`` when neither."""
    value = live.get(key)
    file_name = live.get(f"{key}_file")
    if value is not None and file_name is not None:
        raise LiveVoiceError(f"live.{key} and live.{key}_file are exclusive")
    if file_name is not None:
        return _read_entry_file(entry, file_name, f"{key}_file").strip()
    if value is None:
        return ""
    if not isinstance(value, str):
        raise LiveVoiceError(f"live.{key} is text")
    return value.strip()
