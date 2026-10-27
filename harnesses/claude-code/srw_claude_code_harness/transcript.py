"""JSONL transcript of a session: one ``{"ts", "kind", "payload"}`` object per line.

Every line is flushed as it is written, so a harness that dies mid-run still
leaves a readable transcript. Recording never fails because of the payload:
dataclasses (the SDK's message types) become objects via ``dataclasses.asdict``,
values JSON cannot encode become their ``repr``, and a payload that cannot be
encoded at all (a cycle, non-string keys) is recorded as its ``repr``. Lines are
ASCII-escaped JSON, so text with lone surrogates cannot break the write.
"""

from __future__ import annotations

import dataclasses
import json
from datetime import datetime, timezone
from pathlib import Path


def _repr(value: object) -> str:
    try:
        return repr(value)
    except Exception:  # noqa: BLE001 - a broken __repr__ must not lose the line
        return f"<unrepresentable {type(value).__name__}>"


def _plain(payload: object) -> object:
    if dataclasses.is_dataclass(payload) and not isinstance(payload, type):
        try:
            return dataclasses.asdict(payload)
        except Exception:  # noqa: BLE001 - fall back to repr
            return _repr(payload)
    return payload


def _encode(ts: str, kind: str, payload: object) -> str:
    try:
        return json.dumps(
            {"ts": ts, "kind": kind, "payload": _plain(payload)}, default=_repr
        )
    except Exception:  # noqa: BLE001 - cycles, non-string keys, deep nesting
        return json.dumps({"ts": ts, "kind": kind, "payload": _repr(payload)})


class Transcript:
    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._file = self.path.open("a", encoding="utf-8")

    def record(self, kind: str, payload: object) -> None:
        ts = datetime.now(timezone.utc).isoformat()
        self._file.write(_encode(ts, str(kind), payload) + "\n")
        self._file.flush()

    def close(self) -> None:
        if not self._file.closed:
            self._file.close()
