"""Standard-library logging formatting shared by application adapters.

Applications supply their own correlation-context reader and retain ownership of
logging setup. Importing this module does not configure handlers or capture warnings.
"""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Callable, Mapping
from datetime import datetime, timezone
from typing import Any

REDACTED = "***REDACTED***"

# key: <value> / key=<value> for secret-ish key names.
_KV_SECRET = re.compile(
    r"(?i)\b(authorization|api[_-]?key|secret|client[_-]?secret|password|passwd|"
    r"token|access[_-]?key|private[_-]?key|refresh[_-]?token)"
    r"(\"?\s{0,16}[:=]\s{0,16}\"?)"
    r"([^\s\"',}{)]{1,4096})"
)
# Standalone secret-shaped tokens. Every quantifier is bounded and the JWT is
# anchored by a lookbehind outside its own alphabet: `\b` treats `-` as a
# boundary, so `eyJ-eyJ-…` started a match at every repetition and each one
# rescanned the rest of the record.
_STANDALONE = [
    re.compile(r"(?i)\bbearer\s{1,16}[A-Za-z0-9._\-]{1,4096}"),  # bearer <opaque/jwt>
    re.compile(
        r"(?<![A-Za-z0-9_\-])eyJ[A-Za-z0-9_\-]{1,4096}\.[A-Za-z0-9_\-]{1,4096}"
        r"\.[A-Za-z0-9_\-]{1,4096}"
    ),  # JWT
    re.compile(r"\bsk-[A-Za-z0-9_\-]{16,512}"),  # OpenAI / Anthropic style
    re.compile(r"\bxox[baprs]-[A-Za-z0-9-]{8,512}"),  # Slack
    re.compile(r"\bgh[pousr]_[A-Za-z0-9]{16,255}"),  # GitHub
    # scheme://user:password@host — the userinfo only, so the line still names
    # the host. A failed fetch or push logs its credential-bearing remote, and
    # archived agent logs are served back through get_job_log. Anchored after
    # `://` and stopping at `/`, `@` and whitespace, so it is linear.
    re.compile(r"(?<=://)[^\s/@:]{0,256}+:[^\s/@]{0,2048}+(?=@)"),
]


def redact(text: str) -> str:
    """Mask secret-shaped substrings without introducing quotes or newlines."""
    if not text:
        return text
    text = _KV_SECRET.sub(lambda m: f"{m.group(1)}{m.group(2)}{REDACTED}", text)
    for pattern in _STANDALONE:
        text = pattern.sub(REDACTED, text)
    return text


# Promoted to stable top-level keys, in this order, when present.
_CORRELATION_KEYS = ("request_id", "job_id", "thread_id", "agent_id", "phase")

# Intrinsic LogRecord attributes; anything else on the record is treated as
# structured ``extra=`` and included in the JSON output.
_RESERVED = set(logging.makeLogRecord({}).__dict__.keys()) | {
    "message",
    "asctime",
    "taskName",
    "color_message",  # uvicorn passes an ANSI-laden duplicate of the message
}


class JsonLogFormatter(logging.Formatter):
    """One JSON object per line, using the owning application's current context."""

    def __init__(
        self, *, component: str, context_getter: Callable[[], Mapping[str, Any]]
    ) -> None:
        super().__init__()
        self.component = component
        self._context_getter = context_getter

    def format(self, record: logging.LogRecord) -> str:
        rec: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, timezone.utc)
            .isoformat()
            .replace("+00:00", "Z"),
            "level": record.levelname,
            "logger": record.name,
            "component": self.component,
            "message": redact(record.getMessage()),
            "file": f"{record.filename}:{record.lineno}",
        }
        ctx = self._context_getter()
        for key in _CORRELATION_KEYS:
            if key in ctx:
                rec[key] = ctx[key]
        for key, value in ctx.items():
            rec.setdefault(key, value)
        for key, value in record.__dict__.items():
            if key not in _RESERVED and key not in rec:
                rec[key] = value
        if record.exc_info:
            rec["exc"] = redact(self.formatException(record.exc_info))
        if record.stack_info:
            rec["stack"] = redact(self.formatStack(record.stack_info))
        return json.dumps(rec, default=str, ensure_ascii=False)
