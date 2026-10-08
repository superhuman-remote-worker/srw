#!/usr/bin/env python3
"""example.env/v1: the example bind-time driver (connector drivers D6).

The driver the "Build your own driver" guide (docs/connector-drivers.md)
walks through, the one ``scripts/srw-driver-test.py`` checks in CI, and the
image ``scripts/k3d-custom-driver-gate.py`` registers. Standard library only.

It reads its request from ``SRW_REQUEST_FILE`` (``/run/srw/request.json`` in
its pod) and writes one JSON object per line to stdout:

* ``spec``: the spec its image label declares (``spec.json`` beside it);
* ``check``: SUCCEEDED when the connector holds a token and names a
  variable, else FAILED with what is missing (a wrong config is a result,
  never an error);
* ``bind``: a binding descriptor for the workspace: the variable the
  connector names, set to a credential derived for this binding from the
  connector's token (never the token itself); optionally the same value in a
  credential file; and ``EXAMPLE_DRIVER_PROCESS``, what the kernel says about
  this process (user, capabilities, no-new-privs, seccomp), so a gate can
  read in the workspace how the pod ran. ``driver_state`` names what was
  minted, as a real driver would to revoke it later;
* ``revoke``: succeeds, whether or not the binding still exists;
* ``gc``: retires nothing (it keeps no state of its own);
* anything else: an ``unsupported`` error.

It needs no network: a real driver would call its upstream here (and declare
it as egress in its spec).
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import sys
from pathlib import Path
from typing import Any

SPEC_FILE = Path(__file__).with_name("spec.json")
VARIABLE_FIELD = "variable"
FILE_FIELD = "file"


def emit(line: dict[str, Any]) -> None:
    sys.stdout.write(json.dumps(line, separators=(",", ":")) + "\n")


def result(value: dict[str, Any], *, driver_state: str | None = None) -> int:
    line: dict[str, Any] = {"type": "result", "result": value}
    if driver_state is not None:
        line["driver_state"] = driver_state
    emit(line)
    return 0


def error(error_class: str, message: str, *, field: str | None = None) -> int:
    body: dict[str, Any] = {"class": error_class, "message": message}
    if field is not None:
        body["field"] = field
    emit({"type": "error", "error": body})
    return 1


def spec() -> dict[str, Any]:
    return json.loads(SPEC_FILE.read_text(encoding="utf-8"))


def process_facts() -> str:
    """The kernel's view of this process: user, capabilities, privileges."""
    wanted = ("Uid", "CapEff", "CapBnd", "CapPrm", "NoNewPrivs", "Seccomp")
    facts: dict[str, str] = {}
    try:
        status = Path("/proc/self/status").read_text(encoding="ascii")
    except OSError:
        return "unavailable"
    for line in status.splitlines():
        name, _, value = line.partition(":")
        if name in wanted:
            facts[name] = " ".join(value.split())
    uid = facts.get("Uid", "").split(" ")[0]
    return (
        f"uid={uid} capeff={facts.get('CapEff', '')} capbnd={facts.get('CapBnd', '')} "
        f"capprm={facts.get('CapPrm', '')} nonewprivs={facts.get('NoNewPrivs', '')} "
        f"seccomp={facts.get('Seccomp', '')}"
    )


def minted(token: str, binding_id: str) -> str:
    """A credential for one binding, derived from the connector's token."""
    digest = hmac.new(token.encode(), binding_id.encode(), hashlib.sha256)
    return "example-" + digest.hexdigest()[:32]


def check(request: dict[str, Any]) -> int:
    config = request["connector"]["config"]
    if not request.get("credentials", {}).get("token"):
        return result({"status": "FAILED", "message": "The connector holds no token"})
    if not config.get(VARIABLE_FIELD):
        return result(
            {"status": "FAILED", "message": "The connector names no variable"}
        )
    return result({"status": "SUCCEEDED", "message": "The token is present"})


def bind(request: dict[str, Any]) -> int:
    config = request["connector"]["config"]
    token = request.get("credentials", {}).get("token")
    if not token:
        return error("credentials", "The connector holds no token", field="token")
    variable = config.get(VARIABLE_FIELD)
    if not isinstance(variable, str) or not variable:
        return error("config", "Name the variable to set", field=f"/{VARIABLE_FIELD}")
    value = minted(token, request["binding_id"])
    entries: list[dict[str, Any]] = [
        {
            "recipient": "workspace",
            "form": "env_file",
            "value": {"name": variable, "value": value},
            "collision": "error",
            "refresh": "on_backend_swap",
        },
        {
            "recipient": "workspace",
            "form": "env_file",
            "value": {"name": "EXAMPLE_DRIVER_PROCESS", "value": process_facts()},
            "collision": "error",
        },
    ]
    path = config.get(FILE_FIELD)
    if isinstance(path, str) and path:
        entries.append(
            {
                "recipient": "workspace",
                "form": "credential_file",
                "value": {"path": path, "content": value + "\n", "mode": 0o600},
                "collision": "skip_existing",
                "retire": "remove",
            }
        )
    emit(
        {
            "type": "log",
            "level": "info",
            "message": f"minted a credential for {variable}",
        }
    )
    state = json.dumps({"minted": hashlib.sha256(value.encode()).hexdigest()[:16]})
    return result(
        {
            "binding": {
                "driver": spec()["name"],
                "name": "example",
                "access": request["connector"].get("access"),
                "entries": entries,
            }
        },
        driver_state=state,
    )


def main() -> int:
    path = os.environ.get("SRW_REQUEST_FILE", "/run/srw/request.json")
    try:
        request = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        # stderr reaches the pod log only, never SRW's result.
        print(f"srw-example-driver: {exc}", file=sys.stderr)
        return error("system", "The request file is unreadable")
    operation = request.get("operation")
    if operation == "spec":
        return result(spec())
    if operation == "check":
        return check(request)
    if operation == "bind":
        return bind(request)
    if operation == "revoke":
        # Nothing minted here outlives the binding: already gone is success.
        return result({})
    if operation == "gc":
        return result({"retired": []})
    return error("unsupported", f"The example driver has no {operation!r} operation")


if __name__ == "__main__":
    sys.exit(main())
