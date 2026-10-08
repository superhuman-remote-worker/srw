#!/usr/bin/env python3
"""example.env/v1: the example bind-time driver (connector drivers D6).

The driver the "Build your own driver" guide (docs/connector-drivers.md)
walks through, the one ``scripts/srw-driver-test.py`` checks in CI, and the
image ``scripts/k3d-custom-driver-gate.py`` registers. Standard library only.

It reads its request from ``SRW_REQUEST_FILE`` (``/run/srw/request.json`` in
its pod) and writes one JSON object per line to stdout:

* ``spec``: the spec its image label declares (``spec.json`` beside it);
* ``check``: SUCCEEDED when the connector holds a token, else FAILED with
  what is missing (a wrong config is a result, never an error);
* ``bind``: a binding descriptor for the workspace: ``EXAMPLE_TOKEN``, a
  credential derived for this binding from the connector's token (never the
  token itself); with ``file``, the same value in
  ``~/.srw-files/example/token``, named by ``EXAMPLE_TOKEN_FILE``; and
  ``EXAMPLE_DRIVER_PROCESS``, what the kernel says about this process (user,
  capabilities, no-new-privs, seccomp), so a gate can read in the workspace
  how the pod ran. Every name it sets is declared in ``env_names`` in its
  spec. ``driver_state`` holds what ``revoke`` needs to revoke the minted
  credential (SRW hands it back; a real driver puts the upstream id here);
* ``revoke``: succeeds, whether or not the binding still exists. It checks
  what SRW hands it, as a real driver would need it: the connector's token
  as it was at bind (SRW keeps it on the binding, so a revoke after the
  connector was deleted still has it) and, when given, a ``driver_state``
  that names what this binding minted;
* ``gc``: retires nothing (it keeps no state of its own; SRW never calls it,
  the test kit does);
* anything else: an ``unsupported`` error.

``misbehave`` (config) is for SRW's own gate: it makes ``bind`` return a
variable no driver may set, one the spec does not declare, a file outside
the allowed locations, or fail with a ``config`` error.

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
TOKEN_VARIABLE = "EXAMPLE_TOKEN"
FILE_VARIABLE = "EXAMPLE_TOKEN_FILE"
PROCESS_VARIABLE = "EXAMPLE_DRIVER_PROCESS"
TOKEN_FILE = "~/.srw-files/example/token"


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


def fingerprint(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()[:16]


def revoke(request: dict[str, Any]) -> int:
    token = request.get("credentials", {}).get("token")
    if not token:
        return error("credentials", "Revoke needs the token the binding was made with")
    state = request.get("driver_state")
    if state is None:
        # SRW hands back what the bind returned; without it a real driver
        # could not find what it minted. The gate relies on this failing.
        return error("system", "Revoke needs the driver_state its bind returned")
    try:
        named = json.loads(state).get("minted")
    except (ValueError, AttributeError):
        named = None
    if named != fingerprint(minted(token, request["binding_id"])):
        return error("system", "The driver_state does not name this binding")
    # Nothing minted here outlives the binding: already gone is success.
    return result({})


def check(request: dict[str, Any]) -> int:
    if not request.get("credentials", {}).get("token"):
        return result({"status": "FAILED", "message": "The connector holds no token"})
    return result({"status": "SUCCEEDED", "message": "The token is present"})


def variable(name: str, value: str) -> dict[str, Any]:
    return {
        "recipient": "workspace",
        "form": "env_file",
        "value": {"name": name, "value": value},
        "collision": "error",
        "refresh": "on_backend_swap",
    }


def credential_file(path: str, content: str, env_var: str | None) -> dict[str, Any]:
    value: dict[str, Any] = {"path": path, "content": content, "mode": 0o600}
    if env_var:
        value["env_var"] = env_var
    return {
        "recipient": "workspace",
        "form": "credential_file",
        "value": value,
        "collision": "skip_existing",
        "retire": "remove",
    }


def bind(request: dict[str, Any]) -> int:
    config = request["connector"]["config"]
    token = request.get("credentials", {}).get("token")
    if not token:
        return error("credentials", "The connector holds no token", field="token")
    misbehave = config.get("misbehave")
    if misbehave == "fail":
        return error("config", "Told to fail (misbehave)", field="/misbehave")
    value = minted(token, request["binding_id"])
    entries: list[dict[str, Any]] = [
        variable(TOKEN_VARIABLE, value),
        variable(PROCESS_VARIABLE, process_facts()),
    ]
    if config.get("file"):
        entries.append(credential_file(TOKEN_FILE, value + "\n", FILE_VARIABLE))
    if misbehave == "denied_variable":
        entries.append(variable("GIT_SSH_COMMAND", "true"))
    elif misbehave == "undeclared_variable":
        entries.append(variable("EXAMPLE_UNDECLARED", value))
    elif misbehave == "refused_file":
        entries.append(credential_file("~/.kube/config", "{}\n", None))
    emit({"type": "log", "level": "info", "message": "minted a credential"})
    # What revoke needs to revoke the minted credential upstream.
    state = json.dumps({"minted": fingerprint(value)})
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
        return revoke(request)
    if operation == "gc":
        return result({"retired": []})
    return error("unsupported", f"The example driver has no {operation!r} operation")


if __name__ == "__main__":
    sys.exit(main())
