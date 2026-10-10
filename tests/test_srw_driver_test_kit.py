"""The author test kit and the message JSON Schemas (connector drivers D6).

The example driver (``drivers/example``) passes every step of
``scripts/srw-driver-test.py``: this is the kit's CI run. Broken drivers fail
the step they break, and the shipped schemas agree with the stdlib
validators SRW applies.
"""

from __future__ import annotations

import importlib.util
import json
import sys
import textwrap
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

from shared.connectors.envelope import (
    EnvelopeError,
    parse_output_line,
    validate_request,
)
from shared.connectors.message_schemas import (
    BUILDERS,
    SCHEMA_FILES,
    load_message_schema,
)
from shared.connectors.registration import (
    custom_driver_problems,
    declared_env_names,
    spec_from_json,
)
from shared.connectors.testkit import CommandDriver, Kit

ROOT = Path(__file__).resolve().parents[1]
EXAMPLE = ROOT / "drivers/example"
SPEC = json.loads((EXAMPLE / "spec.json").read_text())
FIXTURE = json.loads((EXAMPLE / "fixture.json").read_text())


def _cli():
    spec = importlib.util.spec_from_file_location(
        "srw_driver_test", ROOT / "scripts/srw-driver-test.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _example(label=SPEC) -> CommandDriver:
    return CommandDriver(
        [sys.executable, str(EXAMPLE / "srw_example_driver.py")], label=label
    )


def _failures(steps) -> dict[str, list[str]]:
    return {step.name: step.problems for step in steps if not step.ok}


class TestTheExampleDriver:
    """CI's run of the kit against SRW's example driver."""

    @pytest.mark.parametrize("label", [SPEC, None], ids=["label", "spec-operation"])
    def test_every_step_passes(self, label):
        steps = Kit(_example(label), FIXTURE, schema_check=_cli().schema_check()).run()
        assert [step.name for step in steps] == [
            "spec",
            "check",
            "bind",
            "revoke",
            "revoke again",
            "gc",
        ]
        assert _failures(steps) == {}

    def test_the_command_line_exits_zero(self, capsys):
        code = _cli().main(
            [
                "--spec-file",
                str(EXAMPLE / "spec.json"),
                "--fixture",
                str(EXAMPLE / "fixture.json"),
                "--json",
                "--",
                sys.executable,
                str(EXAMPLE / "srw_example_driver.py"),
            ]
        )
        assert code == 0
        assert all(step["ok"] for step in json.loads(capsys.readouterr().out))

    def test_its_spec_registers_and_matches_the_spec_schema(self):
        assert (
            custom_driver_problems(
                spec_from_json(SPEC),
                privileged=False,
                env_names=declared_env_names(SPEC),
            )
            == []
        )
        assert (
            list(Draft202012Validator(load_message_schema("spec")).iter_errors(SPEC))
            == []
        )

    def test_a_wrong_check_expectation_fails_check(self):
        steps = Kit(_example(), {**FIXTURE, "credentials": {}}).run()
        failures = _failures(steps)
        assert "check" in failures and "FAILED" in failures["check"][0]
        # A bind without the token is the driver's credentials error.
        assert "credentials error" in failures["bind"][0]

    def test_its_revoke_fails_without_the_driver_state_of_its_bind(self, tmp_path):
        """The k3d gate relies on this: a revoke SRW ran without the
        driver_state its bind returned is an error, never a quiet success."""
        import os
        import subprocess

        request = tmp_path / "request.json"
        request.write_text(
            json.dumps(
                {
                    "protocol_version": "1.0",
                    "operation": "revoke",
                    "binding_id": "6f0d3a52-0d5c-4a4b-9a43-0b8d0a8b2f01",
                    "connector": {"config": {"file": True}, "access": "ReadWrite"},
                    "credentials": {"token": "a-test-token"},
                }
            )
        )
        done = subprocess.run(
            [sys.executable, str(EXAMPLE / "srw_example_driver.py")],
            env={**os.environ, "SRW_REQUEST_FILE": str(request)},
            capture_output=True,
            text=True,
            check=False,
        )
        assert done.returncode == 1
        (line,) = [json.loads(text) for text in done.stdout.splitlines()]
        assert line["type"] == "error"
        assert line["error"]["class"] == "system"
        assert "driver_state" in line["error"]["message"]

    def test_the_image_runs_the_driver_as_a_non_root_user(self):
        dockerfile = (ROOT / "docker/Dockerfile.driver-example").read_text()
        assert (
            "COPY drivers/example/srw_example_driver.py drivers/example/spec.json"
            in (dockerfile)
        )
        assert "USER 10001:10001" in dockerfile
        assert 'ENTRYPOINT ["python3", "/driver/srw_example_driver.py"]' in dockerfile
        # The label comes from spec.json at build time, never a stale copy.
        assert "LABEL io.srw.driver.spec" not in dockerfile


def _driver(tmp_path, body: str) -> CommandDriver:
    path = tmp_path / "driver.py"
    path.write_text(
        textwrap.dedent(
            """
            import json, os, sys
            request = json.load(open(os.environ["SRW_REQUEST_FILE"]))
            op = request["operation"]
            def out(line):
                print(json.dumps(line))
            """
        )
        + textwrap.dedent(body)
    )
    spec = {**SPEC, "operations": []}
    return CommandDriver([sys.executable, str(path)], label=spec)


GOOD = """
if op == "check":
    out({"type": "result", "result": {"status": "SUCCEEDED"}})
elif op == "bind":
    out({"type": "result", "result": {"binding": {"driver": "example.env/v1",
        "name": "x", "access": None, "entries": [ENTRY]}}})
elif op == "revoke":
    out({"type": "result", "result": {}})
else:
    out({"type": "error", "error": {"class": "unsupported", "message": "no"}})
    sys.exit(1)
"""
WORKSPACE_ENV = (
    '{"recipient": "workspace", "form": "env_file", '
    '"value": {"name": "EXAMPLE_TOKEN", "value": "b"}, "collision": "error"}'
)


class TestBrokenDrivers:
    def test_a_well_behaved_driver_without_gc_passes(self, tmp_path):
        driver = _driver(tmp_path, GOOD.replace("ENTRY", WORKSPACE_ENV))
        assert _failures(Kit(driver, FIXTURE).run()) == {}

    def test_a_binding_for_the_harness_fails_bind(self, tmp_path):
        harness = WORKSPACE_ENV.replace('"workspace"', '"harness"')
        driver = _driver(tmp_path, GOOD.replace("ENTRY", harness))
        failures = _failures(Kit(driver, FIXTURE).run())
        assert list(failures) == ["bind"]
        assert "workspace" in failures["bind"][0]

    @pytest.mark.parametrize(
        ("name", "message"),
        [
            ("GIT_SSH_COMMAND", "not a variable a connector may set"),
            ("NODE_OPTIONS", "not a variable a connector may set"),
            ("EXAMPLE_UNDECLARED", "does not declare"),
        ],
    )
    def test_the_kit_runs_srw_s_own_bind_checks(self, tmp_path, name, message):
        """What SRW refuses at bind fails the kit's bind step, word for word."""
        entry = WORKSPACE_ENV.replace("EXAMPLE_TOKEN", name)
        driver = _driver(tmp_path, GOOD.replace("ENTRY", entry))
        failures = _failures(Kit(driver, FIXTURE).run())
        assert list(failures) == ["bind"]
        assert any(message in problem for problem in failures["bind"])

    @pytest.mark.parametrize(
        "path", ["~/.kube/config", "~/.aws/credentials", "~/.config/gcloud/x.json"]
    )
    def test_a_file_in_any_credential_location_passes(self, tmp_path, path):
        """A driver gets the whole credential-file allowlist SRW's own file
        connectors have (connector drivers decision 26)."""
        entry = (
            '{"recipient": "workspace", "form": "credential_file", '
            f'"value": {{"path": "{path}", "content": "{{}}"}}, '
            '"collision": "skip_existing"}'
        )
        driver = _driver(tmp_path, GOOD.replace("ENTRY", entry))
        assert _failures(Kit(driver, FIXTURE).run()) == {}

    @pytest.mark.parametrize(
        ("path", "mode", "message"),
        [
            ("~/.bashrc", 384, "not a credential-file location"),
            ("~/.ssh/config", 384, "not a credential-file location"),
            ("~/.srw-files/x", 493, "never executable"),
        ],
    )
    def test_a_file_outside_them_or_executable_fails_bind(
        self, tmp_path, path, mode, message
    ):
        entry = (
            '{"recipient": "workspace", "form": "credential_file", '
            f'"value": {{"path": "{path}", "content": "x", "mode": {mode}}}, '
            '"collision": "skip_existing"}'
        )
        driver = _driver(tmp_path, GOOD.replace("ENTRY", entry))
        failures = _failures(Kit(driver, FIXTURE).run())
        assert list(failures) == ["bind"]
        assert any(message in problem for problem in failures["bind"])

    def test_revoke_must_succeed_when_already_gone(self, tmp_path):
        marker = str(tmp_path / "revoked")
        revoke_once = textwrap.dedent(
            f"""
            elif op == "revoke":
                if os.path.exists({marker!r}):
                    out({{"type": "error",
                          "error": {{"class": "config", "message": "no such binding"}}}})
                    sys.exit(1)
                open({marker!r}, "w").close()
                out({{"type": "result", "result": {{}}}})
            """
        ).strip("\n")
        body = GOOD.replace("ENTRY", WORKSPACE_ENV).replace(
            'elif op == "revoke":\n    out({"type": "result", "result": {}})',
            revoke_once,
        )
        failures = _failures(Kit(_driver(tmp_path, body), FIXTURE).run())
        assert list(failures) == ["revoke again"]

    def test_an_undeclared_gc_must_be_unsupported(self, tmp_path):
        body = GOOD.replace("ENTRY", WORKSPACE_ENV).replace(
            "else:",
            'elif op == "gc":\n    out({"type": "result", "result": {}})\nelse:',
        )
        failures = _failures(Kit(_driver(tmp_path, body), FIXTURE).run())
        assert list(failures) == ["gc"]

    def test_output_that_breaks_the_protocol_fails_its_step(self, tmp_path):
        body = 'print("not json")\n' + GOOD.replace("ENTRY", WORKSPACE_ENV)
        failures = _failures(Kit(_driver(tmp_path, body), FIXTURE).run())
        assert "check" in failures
        assert any("not JSON" in problem for problem in failures["check"])


class TestTheMessageSchemas:
    @pytest.mark.parametrize("name", sorted(BUILDERS))
    def test_the_shipped_files_are_the_builders(self, name):
        shipped = json.loads(
            (ROOT / "src/shared/connectors" / SCHEMA_FILES[name]).read_text()
        )
        assert shipped == BUILDERS[name]()
        Draft202012Validator.check_schema(shipped)

    @pytest.mark.parametrize(
        "request_",
        [
            {
                "protocol_version": "1.0",
                "operation": "spec",
                "connector": {"config": {}},
                "credentials": {},
            },
            {
                "protocol_version": "1.3",
                "operation": "bind",
                "binding_id": "b",
                "connector": {"config": {}, "access": "ReadOnly"},
                "credentials": {"t": "x"},
                "execution": {"kind": "job", "id": "j"},
            },
            {
                "protocol_version": "1.0",
                "operation": "gc",
                "connector": {"config": {}},
                "credentials": {},
                "live_binding_ids": [],
            },
            {
                "protocol_version": "2.0",
                "operation": "spec",
                "connector": {"config": {}},
                "credentials": {},
            },
            {
                "protocol_version": "1.0",
                "operation": "bind",
                "connector": {"config": {}},
                "credentials": {},
            },
            {
                "protocol_version": "1.0",
                "operation": "gc",
                "connector": {"config": {}},
                "credentials": {},
            },
            {
                "protocol_version": "1.0",
                "operation": "fly",
                "connector": {"config": {}},
                "credentials": {},
            },
            {
                "protocol_version": "1.0",
                "operation": "check",
                "connector": {},
                "credentials": {},
            },
            {
                "protocol_version": "1.0",
                "operation": "check",
                "connector": {"config": {}},
                "credentials": {},
                "execution": {"kind": "cron", "id": "x"},
            },
        ],
    )
    def test_the_request_schema_agrees_with_validate_request(self, request_):
        schema_ok = not list(
            Draft202012Validator(load_message_schema("request")).iter_errors(request_)
        )
        assert schema_ok == (validate_request(request_) == [])

    @pytest.mark.parametrize(
        "line",
        [
            {"type": "result", "result": {}},
            {"type": "result", "result": {}, "driver_state": "s"},
            {"type": "log", "level": "info", "message": "m"},
            {
                "type": "error",
                "error": {"class": "transient", "message": "m", "retry_after_s": 3},
            },
            {"type": "update", "target": "credential", "slot": "token", "value": "v"},
            {"type": "update", "target": "config", "value": {}},
            {"type": "result"},
            {"type": "log", "level": "loud", "message": "m"},
            {"type": "error", "error": {"class": "oops", "message": "m"}},
            {"type": "error", "error": {"class": "config", "message": ""}},
            {"type": "update", "target": "config", "value": "x"},
            {"type": "noise"},
        ],
    )
    def test_the_output_line_schema_agrees_with_parse_output_line(self, line):
        schema_ok = not list(
            Draft202012Validator(load_message_schema("output_line")).iter_errors(line)
        )
        try:
            parse_output_line(json.dumps(line))
            parsed = True
        except EnvelopeError:
            parsed = False
        assert schema_ok == parsed
