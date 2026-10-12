#!/usr/bin/env python3
"""srw-driver-test: run a connector driver through its contract (D6).

The author test kit (``shared.connectors.testkit``) on the command line. It
runs a driver as SRW's shim does, one request file per operation, and checks
spec, check, bind, revoke, revoke again and gc against the rules SRW applies
when it registers and binds the driver, plus the message JSON Schemas
(``src/shared/connectors/*.schema.json``) when ``jsonschema`` is installed.

  # An image, run with docker (no network, the request mounted read-only):
  python3 scripts/srw-driver-test.py --image registry.example/you/driver:1 \\
      --fixture fixture.json

  # A program, run directly (its spec from a file, as its label would carry):
  python3 scripts/srw-driver-test.py --spec-file drivers/example/spec.json \\
      --fixture drivers/example/fixture.json \\
      -- python3 drivers/example/srw_example_driver.py

The fixture is JSON: ``config`` and ``credentials`` as a connector stores
them, optionally ``access``, ``execution`` (``kind``, ``id``, ``project_id``,
``workspace_backend``) and ``expect_check`` (``SUCCEEDED``, the default, or
``FAILED``). Exit status 0 only when every step passes; ``--json`` prints the
steps as JSON.

A fixture's credentials go to the driver you run, nowhere else; use test
credentials.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from shared.connectors.testkit import (  # noqa: E402
    CommandDriver,
    DockerDriver,
    Kit,
    SchemaCheck,
    shipped_schemas,
)


def schema_check() -> SchemaCheck | None:
    """Validation against the shipped message schemas, if ``jsonschema`` is
    installed; the kit's own checks run either way."""
    try:
        from jsonschema import Draft202012Validator
    except ImportError:
        return None
    validators = {
        name: Draft202012Validator(schema) for name, schema in shipped_schemas().items()
    }

    def check(name: str, document: Any) -> list[str]:
        errors = sorted(validators[name].iter_errors(document), key=str)
        return [f"{name} schema: {error.message}" for error in errors[:5]]

    return check


def parse(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a connector driver through its contract.",
        epilog="Put the driver command after --, or name an image with --image.",
    )
    parser.add_argument("--image", help="a driver image, run with docker")
    parser.add_argument(
        "--spec-file",
        type=Path,
        help="the spec JSON a command driver's image label would carry",
    )
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--json", action="store_true", help="print the steps as JSON")
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    if args.command and args.command[0] == "--":
        args.command = args.command[1:]
    if bool(args.image) == bool(args.command):
        parser.error("name an image with --image or a command after --, not both")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse(sys.argv[1:] if argv is None else argv)
    fixture = json.loads(args.fixture.read_text(encoding="utf-8"))
    if args.image:
        driver: Any = DockerDriver(args.image, timeout=args.timeout)
    else:
        label = (
            json.loads(args.spec_file.read_text(encoding="utf-8"))
            if args.spec_file
            else None
        )
        driver = CommandDriver(args.command, label=label, timeout=args.timeout)
    steps = Kit(driver, fixture, schema_check=schema_check()).run()
    if args.json:
        print(
            json.dumps(
                [{"step": s.name, "ok": s.ok, "problems": s.problems} for s in steps],
                indent=2,
            )
        )
    else:
        for step in steps:
            print(f"{'PASS' if step.ok else 'FAIL'}  {step.name}")
            for problem in step.problems:
                print(f"      {problem}")
    return 0 if all(step.ok for step in steps) else 1


if __name__ == "__main__":
    sys.exit(main())
