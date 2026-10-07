"""The driver protocol envelope: one request in, typed JSON lines out.

An image driver reads its request from ``/run/srw/request.json`` and writes
one JSON object per line to stdout; SRW's built-in drivers answer the same
operations in process.  Every line has a ``type``:

* ``result`` — the operation's answer (exactly one on success);
* ``log`` — a ``level`` and ``message`` for the operator log;
* ``error`` — the :class:`DriverError` that ended the operation;
* ``update`` — a rotated credential (``target: credential`` with its
  ``slot``) or a migrated config (``target: config``) for SRW to store.

Exit code 0 means a result was emitted; non-zero means an error was. Only the
``transient`` error class is retried.  ``check`` answers "your config is wrong"
with a result whose ``status`` is ``FAILED``; an ``error`` means the driver
itself broke.

Design: knowledge-base/knowledge/features/connector_drivers.md, "The driver
contract" (the envelope); prior art in the lane 5 research, §12.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Literal, Mapping, get_args

from .binding import validate_binding
from .contract import OPERATIONS, PROTOCOL_VERSION, protocol_supported

ErrorClass = Literal[
    "config", "credentials", "permission", "transient", "unsupported", "system"
]
ERROR_CLASSES: tuple[str, ...] = get_args(ErrorClass)
LineType = Literal["result", "log", "error", "update"]
LINE_TYPES: tuple[str, ...] = get_args(LineType)
CheckStatus = Literal["SUCCEEDED", "FAILED"]
ExecutionKind = Literal["job", "session"]

#: Operations that act on one binding and so need a ``binding_id``.
BINDING_OPERATIONS: frozenset[str] = frozenset({"bind", "revoke", "renew"})
#: Cap on a driver's whole stdout; a longer output is a ``system`` error.
MAX_OUTPUT_BYTES = 1024 * 1024
_LOG_LEVELS = frozenset({"debug", "info", "warning", "error"})


class EnvelopeError(ValueError):
    """A request or an output line that breaks the protocol."""


@dataclass(frozen=True, slots=True)
class DriverError:
    """Why an operation failed.

    ``message`` is shown to the user; ``detail`` is for operators and never
    contains a secret.  ``field`` is a JSON pointer into the config (for
    ``config``) or a credential slot name (for ``credentials``).
    """

    error_class: ErrorClass
    message: str
    detail: str | None = None
    field: str | None = None
    retry_after_s: int | None = None

    @property
    def retryable(self) -> bool:
        return self.error_class == "transient"

    def to_json(self) -> dict[str, Any]:
        out: dict[str, Any] = {"class": self.error_class, "message": self.message}
        for key in ("detail", "field", "retry_after_s"):
            if getattr(self, key) is not None:
                out[key] = getattr(self, key)
        return out

    @classmethod
    def from_json(cls, value: Mapping[str, Any]) -> DriverError:
        if not isinstance(value, Mapping):
            raise EnvelopeError("error must be an object")
        error_class = value.get("class")
        if error_class not in ERROR_CLASSES:
            raise EnvelopeError(
                f"error class {error_class!r} is not one of {ERROR_CLASSES}"
            )
        message = value.get("message")
        if not isinstance(message, str) or not message:
            raise EnvelopeError("error message is required")
        retry = value.get("retry_after_s")
        if retry is not None and (not isinstance(retry, int) or retry < 0):
            raise EnvelopeError("retry_after_s must be a non-negative integer")
        for key in ("detail", "field"):
            if value.get(key) is not None and not isinstance(value[key], str):
                raise EnvelopeError(f"error {key} must be a string")
        return cls(
            error_class=error_class,
            message=message,
            detail=value.get("detail"),
            field=value.get("field"),
            retry_after_s=retry,
        )


@dataclass(frozen=True, slots=True)
class ExecutionRef:
    """The work a binding belongs to."""

    kind: ExecutionKind
    id: str
    project_id: str | None = None
    workspace_backend: str | None = None


@dataclass(frozen=True, slots=True)
class DriverRequest:
    """One operation for a driver.

    ``binding_id`` is stable per binding: ``bind`` is idempotent per id and
    ``revoke`` succeeds when the binding is already gone.  ``driver_state`` is
    the opaque value an earlier ``bind`` or ``renew`` returned; SRW stores it
    encrypted, never shows it to the agent, and hands it back to ``renew`` and
    ``revoke``.  ``gc`` gets ``live_binding_ids`` instead of one binding.
    """

    operation: str
    config: Mapping[str, Any] = field(default_factory=dict)
    access: str | None = None
    credentials: Mapping[str, Any] = field(default_factory=dict, repr=False)
    binding_id: str | None = None
    driver_state: str | None = field(default=None, repr=False)
    execution: ExecutionRef | None = None
    live_binding_ids: tuple[str, ...] = ()
    protocol_version: str = PROTOCOL_VERSION

    def to_json(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "protocol_version": self.protocol_version,
            "operation": self.operation,
            "binding_id": self.binding_id,
            "connector": {"config": dict(self.config), "access": self.access},
            "credentials": dict(self.credentials),
        }
        if self.driver_state is not None:
            out["driver_state"] = self.driver_state
        if self.execution is not None:
            out["execution"] = {
                "kind": self.execution.kind,
                "id": self.execution.id,
                "project_id": self.execution.project_id,
                "workspace_backend": self.execution.workspace_backend,
            }
        if self.operation == "gc":
            out["live_binding_ids"] = list(self.live_binding_ids)
        return out


def validate_request(request: Mapping[str, Any]) -> list[str]:
    """Every problem with a request object, as human-readable lines."""
    if not isinstance(request, Mapping):
        return ["request must be an object"]
    problems: list[str] = []
    if not protocol_supported(str(request.get("protocol_version") or "")):
        problems.append(
            f"protocol_version {request.get('protocol_version')!r} is not supported"
        )
    operation = request.get("operation")
    if operation not in OPERATIONS:
        problems.append(f"operation {operation!r} is not one of {OPERATIONS}")
    binding_id = request.get("binding_id")
    if operation in BINDING_OPERATIONS and not (
        isinstance(binding_id, str) and binding_id
    ):
        problems.append(f"{operation} needs a binding_id")
    connector = request.get("connector")
    if not isinstance(connector, Mapping) or not isinstance(
        connector.get("config"), Mapping
    ):
        problems.append("connector.config must be an object")
    elif connector.get("access") is not None and not isinstance(
        connector["access"], str
    ):
        problems.append("connector.access must be a string or null")
    if not isinstance(request.get("credentials"), Mapping):
        problems.append("credentials must be an object")
    state = request.get("driver_state")
    if state is not None and not isinstance(state, str):
        problems.append("driver_state must be a string")
    execution = request.get("execution")
    if execution is not None and (
        not isinstance(execution, Mapping)
        or execution.get("kind") not in get_args(ExecutionKind)
        or not isinstance(execution.get("id"), str)
    ):
        problems.append("execution needs a kind (job or session) and an id")
    if operation == "gc" and not isinstance(request.get("live_binding_ids"), list):
        problems.append("gc needs live_binding_ids")
    return problems


def parse_output_line(text: str) -> dict[str, Any]:
    """One stdout line as a checked output object.

    Raises :class:`EnvelopeError` when the line is not a JSON object of a
    known type with that type's fields.
    """
    try:
        line = json.loads(text)
    except ValueError as exc:
        raise EnvelopeError("output line is not JSON") from exc
    if not isinstance(line, dict):
        raise EnvelopeError("output line must be a JSON object")
    kind = line.get("type")
    if kind == "result":
        if not isinstance(line.get("result"), dict):
            raise EnvelopeError("a result line carries a result object")
        state = line.get("driver_state")
        if state is not None and not isinstance(state, str):
            raise EnvelopeError("driver_state must be a string")
    elif kind == "log":
        if line.get("level") not in _LOG_LEVELS or not isinstance(
            line.get("message"), str
        ):
            raise EnvelopeError("a log line carries a level and a message")
    elif kind == "error":
        DriverError.from_json(line.get("error"))
    elif kind == "update":
        target = line.get("target")
        if target == "credential":
            if not isinstance(line.get("slot"), str) or "value" not in line:
                raise EnvelopeError("a credential update names its slot and value")
        elif target == "config":
            if not isinstance(line.get("value"), dict):
                raise EnvelopeError("a config update carries the new config object")
        else:
            raise EnvelopeError("an update targets a credential or the config")
    else:
        raise EnvelopeError(f"output line type {kind!r} is not one of {LINE_TYPES}")
    return line


@dataclass(frozen=True, slots=True)
class DriverOutcome:
    """A finished operation: its result or its error, plus logs and updates."""

    result: dict[str, Any] | None = None
    driver_state: str | None = field(default=None, repr=False)
    error: DriverError | None = None
    logs: tuple[dict[str, Any], ...] = ()
    updates: tuple[dict[str, Any], ...] = field(default=(), repr=False)


def _protocol_failure(message: str, logs: list[dict[str, Any]]) -> DriverOutcome:
    return DriverOutcome(
        error=DriverError("system", "The connector driver broke the protocol", message),
        logs=tuple(logs),
    )


def validate_result(operation: str, result: Mapping[str, Any]) -> list[str]:
    """Problems with the result object of one operation (empty when valid).

    ``spec`` results are checked where specs are registered, not here.
    """
    if operation == "check":
        problems = []
        if result.get("status") not in get_args(CheckStatus):
            problems.append("a check result has status SUCCEEDED or FAILED")
        if result.get("message") is not None and not isinstance(result["message"], str):
            problems.append("a check message must be a string")
        return problems
    if operation == "bind":
        return [
            f"binding: {problem}" for problem in validate_binding(result.get("binding"))
        ] + _expiry_problems(result, required=False)
    if operation == "renew":
        return _expiry_problems(result, required=True)
    if operation == "discover" and not isinstance(result.get("items"), list):
        return ["a discover result carries an items list"]
    return []


def _expiry_problems(result: Mapping[str, Any], *, required: bool) -> list[str]:
    problems = []
    for key in ("expires_at", "renew_at"):
        value = result.get(key)
        if value is None:
            if key == "expires_at" and required:
                problems.append("expires_at is required")
        elif not isinstance(value, str):
            problems.append(f"{key} must be an RFC 3339 timestamp string")
    return problems


def read_output(stdout: str, exit_code: int, *, operation: str) -> DriverOutcome:
    """Interpret a driver's whole stdout and exit code for one operation.

    A protocol violation — oversized output, a malformed line, two results,
    an exit code that disagrees with what was emitted, a result of the wrong
    shape for ``operation`` — becomes a ``system`` error, never a partial
    result.
    """
    if len(stdout.encode("utf-8")) > MAX_OUTPUT_BYTES:
        return _protocol_failure(f"output exceeds {MAX_OUTPUT_BYTES} bytes", [])
    results: list[dict[str, Any]] = []
    errors: list[DriverError] = []
    logs: list[dict[str, Any]] = []
    updates: list[dict[str, Any]] = []
    for number, raw in enumerate(stdout.splitlines(), start=1):
        if not raw.strip():
            continue
        try:
            line = parse_output_line(raw)
        except EnvelopeError as exc:
            return _protocol_failure(f"line {number}: {exc}", logs)
        kind = line["type"]
        if kind == "result":
            results.append(line)
        elif kind == "error":
            errors.append(DriverError.from_json(line["error"]))
        elif kind == "log":
            logs.append(line)
        else:
            updates.append(line)
    if exit_code == 0:
        if len(results) != 1 or errors:
            return _protocol_failure(
                "exit 0 needs exactly one result and no error", logs
            )
        problems = validate_result(operation, results[0]["result"])
        if problems:
            return _protocol_failure("; ".join(problems), logs)
        return DriverOutcome(
            result=results[0]["result"],
            driver_state=results[0].get("driver_state"),
            logs=tuple(logs),
            updates=tuple(updates),
        )
    if len(errors) != 1 or results:
        return _protocol_failure(
            f"exit {exit_code} needs exactly one error and no result", logs
        )
    return DriverOutcome(error=errors[0], logs=tuple(logs), updates=tuple(updates))
