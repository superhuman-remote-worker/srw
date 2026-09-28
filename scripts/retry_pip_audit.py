#!/usr/bin/env python3
"""Retry only transient PyPI JSON service failures of the full pip-audit gate."""

from __future__ import annotations

import re
import subprocess
import sys
import time


PYPI_SERVICE_ERROR = re.compile(
    r"^requests\.exceptions\.HTTPError: (?:502|503|504) Server Error: "
    r"[^\n]* for url: https://pypi\.org/pypi/"
    r"[A-Za-z0-9][A-Za-z0-9._-]*/[A-Za-z0-9][A-Za-z0-9._+%-]*/json$",
    re.MULTILINE,
)
VULNERABILITY = re.compile(r"\b(?:CVE-\d{4}-\d+|PYSEC-\d{4}-\d+)\b")
EXCEPTION_CAUSE = "The above exception was the direct cause of the following exception:"
BACKOFF_SECONDS = (1, 2)


def _retryable(returncode: int, stdout: bytes, stderr: bytes) -> bool:
    if returncode != 1 or stdout:
        return False
    diagnostic = stderr.decode("utf-8", errors="replace")
    http_error = PYPI_SERVICE_ERROR.search(diagnostic)
    return bool(
        http_error
        and EXCEPTION_CAUSE in diagnostic[http_error.end() :]
        and diagnostic.strip().splitlines()[-1]
        == "pip_audit._service.interface.ServiceError"
        and not VULNERABILITY.search(diagnostic)
    )


def main(argv: list[str]) -> int:
    if len(argv) < 3 or argv[:2] != ["--", "pip-audit"]:
        print("usage: retry_pip_audit.py -- pip-audit [audit options]", file=sys.stderr)
        return 2

    audit_command = argv[1:]
    for attempt in range(len(BACKOFF_SECONDS) + 1):
        try:
            result = subprocess.run(audit_command, capture_output=True, check=False)
        except OSError:
            print(
                "pip-audit retry wrapper: audit process could not start",
                file=sys.stderr,
            )
            return 127

        sys.stdout.buffer.write(result.stdout)
        sys.stdout.buffer.flush()
        sys.stderr.buffer.write(result.stderr)
        sys.stderr.buffer.flush()

        if result.returncode == 0:
            return 0
        if not _retryable(result.returncode, result.stdout, result.stderr):
            return result.returncode if result.returncode > 0 else 1
        if attempt < len(BACKOFF_SECONDS):
            print(
                f"pip-audit: transient PyPI service failure; retrying full audit "
                f"({attempt + 2}/{len(BACKOFF_SECONDS) + 1})",
                file=sys.stderr,
            )
            time.sleep(BACKOFF_SECONDS[attempt])

    return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
