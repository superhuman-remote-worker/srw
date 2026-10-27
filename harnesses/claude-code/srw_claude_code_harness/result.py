"""``result.json``: the machine-readable record of a harness run (spec B3 step 6)."""

from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path

from .config import HarnessConfig, config_hash, effective_settings
from .session import SessionOutcome
from .task import Task

_RESULT_FIELDS = (
    ("subtype", "subtype"),
    ("isError", "is_error"),
    ("finalMessage", "result"),
    ("numTurns", "num_turns"),
    ("totalCostUsd", "total_cost_usd"),
    ("usage", "usage"),
    ("modelUsage", "model_usage"),
    ("sessionId", "session_id"),
    ("stopReason", "stop_reason"),
    ("durationMs", "duration_ms"),
    ("durationApiMs", "duration_api_ms"),
    ("apiErrorStatus", "api_error_status"),
)


def write_result(
    path: Path,
    *,
    config_data: object,
    config: HarnessConfig,
    task: Task,
    outcome: SessionOutcome | None,
    started: datetime,
    finished: datetime,
    exit_code: int,
    exit_reason: str,
    inputs: list[dict],
    archive: dict | None,
    versions: dict,
) -> dict:
    message = getattr(outcome, "result", None) if outcome is not None else None
    data = {
        "version": 1,
        "exitCode": exit_code,
        "exitReason": exit_reason,
        "startedAt": started.isoformat(),
        "finishedAt": finished.isoformat(),
        "durationSeconds": (finished - started).total_seconds(),
        "config": effective_settings(config),
        "configHash": config_hash(config_data),
        "prompt": task.prompt,
        "inputs": inputs,
        "archive": archive,
        "versions": {
            "harness": versions.get("harness"),
            "sdk": versions.get("sdk"),
            "cli": versions.get("cli"),
        },
        "result": (
            {key: getattr(message, attr, None) for key, attr in _RESULT_FIELDS}
            if message is not None
            else None
        ),
        "timedOut": bool(getattr(outcome, "timed_out", False)),
        "interrupted": bool(getattr(outcome, "interrupted", False)),
        "error": getattr(outcome, "error", None),
    }
    text = json.dumps(data, indent=2, default=str)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(text, encoding="utf-8")
    os.replace(temporary, path)
    return json.loads(text)
