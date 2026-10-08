"""The author test kit: run a driver through its contract (D6).

SRW checks no image at admission; a bad driver fails at ``check`` or
``bind`` and the connector shows its error. Authors get this kit instead
(``scripts/srw-driver-test.py`` is its command line): it runs a driver as
SRW's shim does, one request file per operation, and checks what it writes
against the same rules SRW applies:

1. **spec**: the ``io.srw.driver.spec`` label (else the ``spec``
   operation's result) reads (:func:`.registration.spec_from_json`) and
   registers (:func:`.registration.custom_driver_problems`);
2. **check**: a result with the status the fixture expects;
3. **bind**: a binding descriptor SRW delivers
   (:func:`.registration.image_binding_problems`), and its ``driver_state``;
4. **revoke** with that state, then **revoke again**: both succeed (revoke is
   idempotent: already gone is success);
5. **gc** with no live binding: a result when the spec declares ``gc``,
   else an ``unsupported`` error.

Every request is checked before it is sent (:func:`.envelope.validate_request`)
and every output by :func:`.envelope.read_output`. A :data:`SchemaCheck`
(the command line builds one with ``jsonschema`` when it is installed) checks
the shipped message schemas too. Standard library only, like the rest of the
contract.
"""

from __future__ import annotations

import json
import os
import subprocess
import tempfile
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .envelope import (
    DriverOutcome,
    DriverRequest,
    ExecutionRef,
    read_output,
    validate_request,
)
from .images import SPEC_LABEL, label_spec
from .registration import (
    custom_driver_problems,
    declared_env_names,
    image_binding_problems,
    spec_from_json,
)

#: Validates a document against a named message schema; returns problems.
SchemaCheck = Callable[[str, Any], list[str]]


@dataclass
class Step:
    """One checked step and why it failed (empty when it passed)."""

    name: str
    problems: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.problems


class CommandDriver:
    """A driver program run directly, as the shim runs it in a pod."""

    def __init__(
        self,
        command: Sequence[str],
        *,
        label: Mapping[str, Any] | None = None,
        timeout: float = 60.0,
    ) -> None:
        self.command = list(command)
        self.label = label
        self.timeout = timeout

    def spec_label(self) -> dict[str, Any] | None:
        return dict(self.label) if self.label is not None else None

    def run(self, request: Mapping[str, Any]) -> tuple[str, int]:
        with tempfile.TemporaryDirectory(prefix="srw-driver-test-") as directory:
            path = Path(directory, "request.json")
            path.write_text(json.dumps(request), encoding="utf-8")
            done = subprocess.run(
                self.command,
                env={"PATH": os.environ.get("PATH", ""), "SRW_REQUEST_FILE": str(path)},
                stdin=subprocess.DEVNULL,
                capture_output=True,
                text=True,
                timeout=self.timeout,
                check=False,
            )
        return done.stdout, done.returncode


class DockerDriver:
    """A driver image run with docker: no network, the request mounted
    read-only where SRW mounts it."""

    def __init__(self, image: str, *, timeout: float = 120.0) -> None:
        self.image = image
        self.timeout = timeout

    def spec_label(self) -> dict[str, Any] | None:
        done = subprocess.run(
            [
                "docker",
                "image",
                "inspect",
                "--format",
                "{{json .Config.Labels}}",
                self.image,
            ],
            capture_output=True,
            text=True,
            timeout=self.timeout,
            check=True,
        )
        labels = json.loads(done.stdout or "null") or {}
        return label_spec(labels)

    def run(self, request: Mapping[str, Any]) -> tuple[str, int]:
        with tempfile.TemporaryDirectory(prefix="srw-driver-test-") as directory:
            Path(directory, "request.json").write_text(
                json.dumps(request), encoding="utf-8"
            )
            os.chmod(directory, 0o755)
            os.chmod(Path(directory, "request.json"), 0o644)
            done = subprocess.run(
                [
                    "docker",
                    "run",
                    "--rm",
                    "--network",
                    "none",
                    "--cap-drop",
                    "ALL",
                    "--security-opt",
                    "no-new-privileges",
                    "-v",
                    f"{directory}:/run/srw:ro",
                    "-e",
                    "SRW_REQUEST_FILE=/run/srw/request.json",
                    self.image,
                ],
                stdin=subprocess.DEVNULL,
                capture_output=True,
                text=True,
                timeout=self.timeout,
                check=False,
            )
        return done.stdout, done.returncode


def shipped_schemas() -> dict[str, dict[str, Any]]:
    """The message schemas a :data:`SchemaCheck` validates against, by name:
    ``request``, ``output_line``, ``spec`` and ``binding``."""
    from .binding import load_binding_schema
    from .message_schemas import load_message_schema

    return {
        "request": load_message_schema("request"),
        "output_line": load_message_schema("output_line"),
        "spec": load_message_schema("spec"),
        "binding": load_binding_schema(),
    }


class Kit:
    """Runs the steps against one driver and one fixture."""

    def __init__(
        self,
        driver: Any,
        fixture: Mapping[str, Any],
        *,
        schema_check: SchemaCheck | None = None,
    ) -> None:
        self.driver = driver
        self.fixture = fixture
        self.schema_check = schema_check
        self.steps: list[Step] = []
        #: The variable names the driver's spec declares (``env_names``).
        self.env_names: tuple[str, ...] = ()

    def _request(self, operation: str, **fields: Any) -> dict[str, Any]:
        execution = self.fixture.get("execution") or {}
        request = DriverRequest(
            operation=operation,
            config=dict(self.fixture.get("config") or {}),
            access=self.fixture.get("access"),
            credentials=dict(self.fixture.get("credentials") or {}),
            execution=ExecutionRef(
                kind=execution.get("kind", "session"),
                id=str(execution.get("id", "srw-driver-test")),
                project_id=execution.get("project_id"),
                workspace_backend=execution.get("workspace_backend", "sandbox"),
            ),
            **fields,
        ).to_json()
        return request

    def _call(self, step: Step, request: dict[str, Any]) -> DriverOutcome | None:
        step.problems += [f"request: {p}" for p in validate_request(request)]
        if self.schema_check is not None:
            step.problems += self.schema_check("request", request)
        try:
            stdout, code = self.driver.run(request)
        except (OSError, subprocess.SubprocessError) as exc:
            step.problems.append(f"the driver did not run: {exc}")
            return None
        if self.schema_check is not None:
            for number, line in enumerate(stdout.splitlines(), start=1):
                if not line.strip():
                    continue
                try:
                    document = json.loads(line)
                except ValueError:
                    continue  # read_output reports it
                step.problems += [
                    f"line {number}: {p}"
                    for p in self.schema_check("output_line", document)
                ]
        outcome = read_output(stdout, code, operation=request["operation"])
        if outcome.error is not None and outcome.error.error_class == "system":
            if "protocol" in outcome.error.message:
                step.problems.append(f"output: {outcome.error.detail}")
        return outcome

    def _expect_result(self, step: Step, outcome: DriverOutcome | None) -> bool:
        if outcome is None:
            return False
        if outcome.error is not None:
            step.problems.append(
                f"expected a result, got a {outcome.error.error_class} error: "
                f"{outcome.error.message}"
            )
            return False
        return True

    def spec(self) -> Any:
        step = Step("spec")
        self.steps.append(step)
        spec_json: Any = None
        try:
            spec_json = self.driver.spec_label()
        except (ValueError, OSError, subprocess.SubprocessError) as exc:
            step.problems.append(f"the {SPEC_LABEL} label is unreadable: {exc}")
            return None
        source = "label"
        if spec_json is None:
            source = "the spec operation"
            outcome = self._call(step, self._request("spec"))
            if not self._expect_result(step, outcome):
                return None
            spec_json = outcome.result
        if self.schema_check is not None:
            step.problems += self.schema_check("spec", spec_json)
        try:
            spec = spec_from_json(spec_json)
        except ValueError as exc:
            step.problems.append(f"the spec from {source} does not read: {exc}")
            return None
        self.env_names = declared_env_names(spec_json)
        step.problems += [
            f"registration: {p}"
            for p in custom_driver_problems(
                spec, privileged=False, env_names=self.env_names
            )
        ]
        return spec

    def check(self) -> None:
        step = Step("check")
        self.steps.append(step)
        outcome = self._call(step, self._request("check"))
        if not self._expect_result(step, outcome):
            return
        expected = self.fixture.get("expect_check", "SUCCEEDED")
        status = (outcome.result or {}).get("status")
        if status != expected:
            step.problems.append(
                f"check answered {status!r} ({(outcome.result or {}).get('message')}), "
                f"the fixture expects {expected!r}"
            )

    def bind(self, spec: Any, binding_id: str) -> str | None:
        step = Step("bind")
        self.steps.append(step)
        outcome = self._call(step, self._request("bind", binding_id=binding_id))
        if not self._expect_result(step, outcome):
            return None
        descriptor = (outcome.result or {}).get("binding")
        if self.schema_check is not None:
            step.problems += self.schema_check("binding", descriptor)
        if spec is not None:
            step.problems += [
                f"binding: {p}"
                for p in image_binding_problems(
                    descriptor, spec, env_names=self.env_names
                )
            ]
        return outcome.driver_state

    def revoke(self, name: str, binding_id: str, state: str | None) -> None:
        step = Step(name)
        self.steps.append(step)
        outcome = self._call(
            step,
            self._request("revoke", binding_id=binding_id, driver_state=state),
        )
        self._expect_result(step, outcome)

    def gc(self, spec: Any) -> None:
        step = Step("gc")
        self.steps.append(step)
        outcome = self._call(step, self._request("gc", live_binding_ids=()))
        if outcome is None:
            return
        declared = spec is not None and "gc" in spec.operations
        if declared:
            self._expect_result(step, outcome)
        elif outcome.error is None or outcome.error.error_class != "unsupported":
            step.problems.append(
                "gc is not declared in the spec's operations, so it must answer "
                "an unsupported error"
            )

    def run(self) -> list[Step]:
        spec = self.spec()
        self.check()
        binding_id = f"srw-driver-test-{uuid.uuid4()}"
        state = self.bind(spec, binding_id)
        self.revoke("revoke", binding_id, state)
        self.revoke("revoke again", binding_id, state)
        self.gc(spec)
        return self.steps


__all__ = [
    "CommandDriver",
    "DockerDriver",
    "Kit",
    "SchemaCheck",
    "Step",
    "shipped_schemas",
]
