#!/usr/bin/env python3
"""Local k3d gate for connector drivers D3b: Connector secrets.

Design: knowledge-base/knowledge/features/connector_drivers.md, "Datasources
become Connectors" (the mapping's secret rows, Release N, "Carried
questions" -> Credentials), decision 11 and Track D, D3 (the D3b part).
Template: scripts/k3d-connector-resources-gate.py (D3a), with the same
safety envelope: dry-run by default, the exact k3d-srw/srw context,
passwords and secret values only on ``kubectl exec -i`` stdin and scrubbed
from every printed line, ``cap_memory()`` in every program the gate runs in
a pod, and a cleanup in ``finally`` that touches only what this run created,
followed by a residue check by gate id.

No program the gate runs in a pod prints a secret value: they report key
names, scopes, versions and booleans ("the secret rebuilds the row").  The
leak checks count the gate's own secret values in raw text and print only
the count.

Fixtures (all disposable, all named after the gate id):

  postgres   a database and a login role on srw-postgres (SELECT on one
             marker table); every Postgres connector's URL carries the role's
             password, which the resource never may
  project    one project owned by the owner account (``--user``) with its
             native knowledge base, and the second account (``--other-user``)
             added as an editor; the third account (``--stranger-user``) is
             not a member
  legacy     one Postgres row written straight to the table, as an
             orchestrator without the write-through would; one connector
             written through the API whose resource is then put back to its
             D3a shape (no ``spec.credentials``, no secret)

Checks (each printed PASS/FAIL; the exit status is 0 only if all pass):

  preflight  Tilt reports the srw resource ``ok``; every orchestrator pod
             serves this checkout's D3b modules byte for byte; the three
             accounts are approved, the owner may publish, the others are not
             administrators
  backfill   the deployed ``migrate_stored_connectors``, rerun in the
             orchestrator pod, gives the legacy row its Connector and its
             secret and restores the D3a-shaped one: each of this run's
             Connectors then has exactly one ``connector-<32 hex>`` secret,
             in the Connector's scope, whose keys are the ones its driver's
             slots name (stated per fixture), that the resource's
             ``spec.credentials`` names and that rebuild the row's
             credentials and full URL; the project's own knowledge base's
             secret is in the project's scope; a rerun changes no secret
  write      through the API: a create writes the secret; an edit without
             credentials, or with an empty object, keeps it (same version);
             a ``credentials`` edit merges (a variable it does not name
             stays), a ``generic`` edit replaces (one it does not name goes);
             a URL edit reaches it; a delete removes it with the row; the
             resource API refuses to write a ``connector-`` secret
  shared     a public and a project-linked Postgres connector of the owner,
             each secret's URL marked (an ``application_name``) so it differs
             from its row: the other account's session selecting both is
             admitted; the deployed delivery path (the application's own
             composition) delivers both to that session from their secrets;
             after one turn the agent logs a connection to each
  refused    the stranger's session selecting the linked or a private
             connector is refused at creation, and the deployed delivery path
             refuses a stranger's session that names the linked connector
  leaks      none of the gate's secret values appears in this run's
             ``srw_resource_revisions`` or ``srw_resources`` rows, in any
             connector or resource API response (reads, lists, Test, the
             driver matrix), or in the orchestrator's or the stateless agents'
             logs since the gate started
  cleanup    nothing this run created is left: sessions titled with the gate
             id and their pods, connectors, their Connector resources and
             secrets, the project, the database and the role

Run with the repository venv on the k3d-srw cluster, alone: this is a
mutating gate. The owner must be able to publish (an administrator or the
``public_datasources`` grant); the other two accounts must be approved
non-administrators.

  .venv/bin/python scripts/k3d-connector-secrets-gate.py           # plan
  .venv/bin/python scripts/k3d-connector-secrets-gate.py \\
      --run --confirm LOCAL-K3D-DISPOSABLE
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import secrets
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parents[1]
LOCAL_CONTEXT = "k3d-srw"
LOCAL_NAMESPACE = "srw"
LOCAL_CONFIRMATION = "LOCAL-K3D-DISPOSABLE"
K = ["kubectl", f"--context={LOCAL_CONTEXT}", "-n", LOCAL_NAMESPACE]
ORCHESTRATOR = "deploy/srw-orchestrator"
ORCHESTRATOR_CONTAINER = "orchestrator"
POSTGRES_POD = "srw-postgres-0"
KEYCLOAK_TOKEN_URL = "http://srw-keycloak:8080/realms/srw/protocol/openid-connect/token"
POD_ROOT = "/app"
PG_HOST = "srw-postgres"
DEFAULT_MODEL = "muse-spark-1.3-contributor"
_SELECTOR = (
    "app.kubernetes.io/instance=srw,app.kubernetes.io/name=superhuman-remote-worker"
)
_GATE_ID_RE = re.compile(r"d3b-[0-9a-f]{10}\Z")
_MODEL_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/@-]{0,199}\Z")
_USER_RE = re.compile(r"[a-z][a-z0-9._-]{0,63}\Z")
_UUID_RE = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\Z")

#: What the orchestrator must serve byte for byte for D3b.
SERVED = (
    "src/orchestrator/services/connector_secrets.py",
    "src/orchestrator/services/manifest_connectors.py",
    "src/orchestrator/services/manifest_authority.py",
    "src/orchestrator/services/manifest_resolution.py",
    "src/orchestrator/services/manifest_resources.py",
    "src/orchestrator/services/manifest_execution.py",
    "src/orchestrator/services/job_datasource_selection.py",
    "src/orchestrator/services/thread_datasource_authorization.py",
    "src/orchestrator/services/datasources.py",
    "src/orchestrator/services/connector_drivers/base.py",
    "src/orchestrator/services/connector_drivers/env.py",
    "src/orchestrator/services/connector_drivers/mcp_client.py",
    "src/orchestrator/services/connector_drivers/repository.py",
    "src/orchestrator/services/connector_drivers/kb.py",
    "src/orchestrator/services/connector_drivers/mail.py",
    "src/orchestrator/services/connector_drivers/credential_files.py",
    "src/orchestrator/services/connector_drivers/ssh_key.py",
    "src/orchestrator/application/preparation.py",
    "src/orchestrator/application/sessions.py",
    "src/orchestrator/application/projects.py",
    "src/orchestrator/database/postgres.py",
    "src/shared/connectors/builtin.py",
)
SECRET_PREFIX = "connector-"
URL_KEY = "url"
SHAPE_KEY = "shape"
CONNECTOR_SECRET_DETAIL = (
    "This name belongs to a connector's credentials; change them on the "
    "Connectors page (/api/datasources), which writes the connector and its "
    "secret together."
)
UNAVAILABLE_DETAIL = "One or more selected connectors are unavailable"
_SECRETS: list[str] = []


class GateError(RuntimeError):
    """Infrastructure trouble: the gate could not observe the product."""


class SafetyError(RuntimeError):
    """The requested run is outside the local disposable boundary."""


def _scrub(text: str) -> str:
    for value in _SECRETS:
        if value:
            text = text.replace(value, "<redacted>")
    return text


def secret(value: str) -> str:
    _SECRETS.append(value)
    return value


def leak_count(text: str) -> int:
    """How often a value this run registered as secret occurs in ``text``."""
    return sum(text.count(value) for value in _SECRETS if value)


def _label(args: list[str]) -> str:
    return " ".join(args[4:7]) if args[:1] == ["kubectl"] else " ".join(args[:2])


def run_raw(
    args: list[str], *, data: str | None = None, timeout: int = 180
) -> tuple[int, str, str]:
    """Run argv; never echo argv or stdin.  The output is NOT scrubbed: only
    the leak checks read it, and they print counts."""
    try:
        result = subprocess.run(
            args, input=data, text=True, capture_output=True, timeout=timeout
        )
    except subprocess.TimeoutExpired:
        raise GateError(f"{_label(args)[:80]} timed out after {timeout}s") from None
    return result.returncode, result.stdout, result.stderr


def run(
    args: list[str], *, data: str | None = None, timeout: int = 180
) -> tuple[int, str, str]:
    """Run argv; never echo argv or stdin. Output comes back scrubbed."""
    rc, out, err = run_raw(args, data=data, timeout=timeout)
    return rc, _scrub(out.strip()), _scrub(err)


def command(args: list[str], *, data: str | None = None, timeout: int = 180) -> str:
    rc, out, err = run(args, data=data, timeout=timeout)
    if rc:
        raise GateError(f"{_label(args)[:80]} failed (exit {rc}): {err.strip()[-400:]}")
    return out


def _psql(database: str) -> list[str]:
    return K + ["exec", POSTGRES_POD, "--", "psql", "-U", "srw", "-d", database]


def sql(query: str, *, database: str = "srw") -> str:
    """One statement on the app database (no secret may be in ``query``)."""
    return command(_psql(database) + ["-v", "ON_ERROR_STOP=1", "-tAc", query])


def sql_raw(query: str) -> str:
    """``sql`` without scrubbing, for the leak checks only; never printed."""
    rc, out, err = run_raw(_psql("srw") + ["-v", "ON_ERROR_STOP=1", "-tAc", query])
    if rc:
        raise GateError(f"psql failed (exit {rc}): {_scrub(err).strip()[-400:]}")
    return out


def sql_script(script: str, *, database: str = "srw") -> str:
    """A script on stdin, so a password in it never becomes an argument."""
    return command(
        K
        + ["exec", "-i", POSTGRES_POD, "--", "psql", "-U", "srw", "-d", database]
        + ["-v", "ON_ERROR_STOP=1", "-tAq", "-f", "-"],
        data=script,
    )


def sql_json(query: str) -> Any:
    out = sql(query)
    return json.loads(out) if out else None


def lit(text: str) -> str:
    return "'" + str(text).replace("'", "''") + "'"


def wait_for(
    label: str, probe: Callable[[], Any], *, timeout: int, interval: float = 3.0
):
    deadline = time.monotonic() + timeout
    while True:
        value = probe()
        if value:
            return value
        if time.monotonic() >= deadline:
            raise GateError(f"timed out: {label}")
        time.sleep(interval)


# ---------------------------------------------------------------------------
# Programs run inside the orchestrator container (stdin carries every secret)
# ---------------------------------------------------------------------------

# Every program the gate runs inside a pod starts with this and calls
# cap_memory() once its imports are done (the same helper as the D1a and D3a
# gates): the orchestrator pod has a 1 GiB limit and serves the product.
POD_MEMORY_BUDGET = 256 << 20
_POD_MEMORY_CAP = (
    r"""
import resource as _srw_resource
def cap_memory(budget=%d):
    with open("/proc/self/status") as status:
        data = next(
            int(line.split()[1]) * 1024
            for line in status
            if line.startswith("VmData:")
        )
    limit = data + budget
    _soft, hard = _srw_resource.getrlimit(_srw_resource.RLIMIT_DATA)
    if hard != _srw_resource.RLIM_INFINITY:
        limit = min(limit, hard)  # an inherited cap is only ever tightened
    _srw_resource.setrlimit(_srw_resource.RLIMIT_DATA, (limit, limit))
"""
    % POD_MEMORY_BUDGET
)

# An API call as one account. ``needles`` are counted in the raw body before
# anything is printed; the gate scrubs the body it prints as well.
_API_PROGRAM = (
    _POD_MEMORY_CAP
    + r"""
import json, sys, urllib.error, urllib.parse, urllib.request
cap_memory()
envelope = json.loads(sys.stdin.readline())
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
form = urllib.parse.urlencode({
    "grant_type": "password", "client_id": "admin-cli", "scope": "openid",
    "username": envelope["username"], "password": envelope["password"],
}).encode()
with opener.open(envelope["token_url"], data=form, timeout=30) as response:
    token = json.load(response)["id_token"]
body = envelope.get("body")
request = urllib.request.Request(
    "http://localhost:8085" + envelope["path"],
    data=None if body is None else json.dumps(body).encode(),
    method=envelope["method"],
    headers={"Authorization": "Bearer " + token, "Content-Type": "application/json"},
)
try:
    with opener.open(request, timeout=180) as response:
        status, text = response.status, response.read().decode("utf-8", "replace")
except urllib.error.HTTPError as error:
    status, text = error.code, error.read().decode("utf-8", "replace")
leaks = sum(text.count(n) for n in envelope.get("needles", []) if n)
for needle in envelope.get("needles", []):
    if needle:
        text = text.replace(needle, "<redacted>")
print(json.dumps({"status": status, "body": text, "leaks": leaks}))
"""
)

# The deployed startup backfill, rerun: idempotent.
_BACKFILL_PROGRAM = (
    _POD_MEMORY_CAP
    + r"""
import asyncio, json, logging, sys
from orchestrator.database.postgres import PostgresDB
from orchestrator.services.manifest_connectors import migrate_stored_connectors
cap_memory()
logging.disable(logging.CRITICAL)
async def main():
    db = PostgresDB(min_connections=1, max_connections=2)
    await db.connect()
    try:
        counts = await migrate_stored_connectors(db)
    finally:
        await db.close()
    print(json.dumps(counts))
asyncio.run(main())
"""
)

# Each connector's resource and secret, as names, scopes, versions and
# booleans. The secret is decrypted only to compare it with the row and with
# what the deployed mapping derives from the row; no value is printed.
_SECRETS_PROGRAM = (
    _POD_MEMORY_CAP
    + r"""
import asyncio, json, logging, sys
from uuid import UUID
from orchestrator.database.postgres import PostgresDB
from orchestrator.security.crypto import decrypt
from orchestrator.services.connector_drivers import builtin_connector_drivers
from orchestrator.services.connector_secrets import (
    connector_secret_name, secret_values, stored_credentials,
)
cap_memory()
logging.disable(logging.CRITICAL)
request = json.loads(sys.stdin.readline())
registry = builtin_connector_drivers()
async def one(db, datasource_id):
    name = connector_secret_name(datasource_id)
    row = await db.get_datasource(datasource_id)
    resource = await db.fetchrow(
        "SELECT scope_kind, scope_name, deleted_at, "
        "document->'spec'->'credentials' AS refs FROM srw_resources WHERE id=$1",
        UUID(datasource_id),
    )
    found = await db.fetch(
        "SELECT scope_kind, scope_name, keys, version, owner_id, ciphertext "
        "FROM srw_resource_secrets WHERE name=$1 ORDER BY scope_kind, scope_name",
        name,
    )
    refs = None
    if resource is not None and resource["refs"] is not None:
        refs = json.loads(resource["refs"])
    entry = {
        "row": row is not None,
        "resource": None if resource is None else {
            "scope": [resource["scope_kind"], resource["scope_name"]],
            "deleted": resource["deleted_at"] is not None,
            "refs": None if refs is None else sorted(refs),
            "refs_own": refs is not None and all(
                ref == {"secretRef": {"name": name, "key": key}}
                for key, ref in refs.items()
            ),
        },
        "secrets": [
            {
                "scope": [s["scope_kind"], s["scope_name"]],
                "keys": sorted(s["keys"]),
                "version": s["version"],
                "owner": None if s["owner_id"] is None else str(s["owner_id"]),
            }
            for s in found
        ],
    }
    if row is not None:
        driver = registry.for_type(row["type"])
        entry["derived_keys"] = (
            sorted(secret_values(driver, row)) if driver is not None else None
        )
        own = [
            s for s in found
            if resource is not None
            and [s["scope_kind"], s["scope_name"]] == entry["resource"]["scope"]
        ]
        values = json.loads(decrypt(own[0]["ciphertext"])) if own else {}
        entry["rebuilds_row"] = stored_credentials(values) == (
            row["credentials"], row["connection_url"]
        )
        entry["matches_mapping"] = driver is not None and values == secret_values(
            driver, row
        )
    return entry
async def main():
    db = PostgresDB(
        min_connections=1, max_connections=1,
        server_settings={"default_transaction_read_only": "on"},
    )
    await db.connect()
    try:
        out = {i: await one(db, i) for i in request["ids"]}
    finally:
        await db.close()
    print(json.dumps(out))
asyncio.run(main())
"""
)

# Mark each named connector's secret: its URL gains an application_name, so
# it differs from the row, and a delivery that carries the mark read the
# secret. The resource stays in step with its row.
_MARK_PROGRAM = (
    _POD_MEMORY_CAP
    + r"""
import asyncio, json, logging, sys
from uuid import UUID
from orchestrator.database.postgres import PostgresDB
from orchestrator.security.crypto import decrypt
from orchestrator.services.connector_secrets import (
    connector_secret_name, write_connector_secret,
)
from orchestrator.services.manifest_store import ManifestStore
cap_memory()
logging.disable(logging.CRITICAL)
request = json.loads(sys.stdin.readline())
async def main():
    db = PostgresDB(min_connections=1, max_connections=1)
    await db.connect()
    marked = []
    try:
        for datasource_id in request["ids"]:
            async with db.transaction_scope():
                await ManifestStore(db).lock_catalog()
                resource = await db.fetchrow(
                    "SELECT scope_kind, scope_name FROM srw_resources "
                    "WHERE id=$1 AND deleted_at IS NULL",
                    UUID(datasource_id),
                )
                found = resource and await db.fetchrow(
                    "SELECT ciphertext, owner_id FROM srw_resource_secrets WHERE "
                    "scope_kind=$1 AND scope_name=$2 AND name=$3 FOR UPDATE",
                    resource["scope_kind"], resource["scope_name"],
                    connector_secret_name(datasource_id),
                )
                if not found:
                    continue
                values = json.loads(decrypt(found["ciphertext"]))
                url = values["url"]
                values["url"] = (
                    url + ("&" if "?" in url else "?")
                    + "application_name=" + request["marker"]
                )
                await write_connector_secret(
                    db, datasource_id,
                    {"kind": resource["scope_kind"], "name": resource["scope_name"]},
                    owner_id=found["owner_id"], values=values,
                )
                marked.append(datasource_id)
    finally:
        await db.close()
    print(json.dumps({"marked": marked}))
asyncio.run(main())
"""
)

# The deployed delivery path for one session, through the application's own
# composition: authorize, resolve, read the secrets, build the payload. It
# prints names, and which delivered URL carries the mark; never a value.
_DELIVERY_PROGRAM = (
    _POD_MEMORY_CAP
    + r"""
import asyncio, json, logging, sys
from types import SimpleNamespace
from fastapi import HTTPException
from orchestrator.application.preparation import datasource_payload_dependencies
from orchestrator.application.sessions import (
    thread_datasource_authorization_dependencies,
)
from orchestrator.application.settings import DeploymentSettings
from orchestrator.database.postgres import PostgresDB
from orchestrator.services.agent_datasource_payload import build_datasources_payload
from orchestrator.services.connector_drivers.registry import builtin_connector_drivers
from orchestrator.services.thread_datasource_authorization import (
    resolve_authorized_thread_datasources,
)
cap_memory()
logging.disable(logging.CRITICAL)
request = json.loads(sys.stdin.readline())
async def main():
    db = PostgresDB(
        min_connections=1, max_connections=2,
        server_settings={"default_transaction_read_only": "on"},
    )
    await db.connect()
    try:
        thread = request.get("thread") or await db.get_thread(request["thread_id"])
        try:
            rows = await resolve_authorized_thread_datasources(
                thread,
                request["datasource_ids"],
                target_project_ids=request["project_ids"],
                dependencies=thread_datasource_authorization_dependencies(
                    SimpleNamespace(postgres_db=db)
                ),
            )
        except HTTPException as exc:
            print(json.dumps({"status": exc.status_code, "detail": str(exc.detail)}))
            return
    finally:
        await db.close()
    deps = datasource_payload_dependencies(
        SimpleNamespace(
            connector_drivers=builtin_connector_drivers(),
            settings=DeploymentSettings.from_environment(),
        )
    )
    payload = build_datasources_payload(rows, dependencies=deps) or []
    marker = "application_name=" + request["marker"]
    print(json.dumps({
        "status": 200,
        "delivered": sorted(e.get("name") for e in payload),
        "from_secret": sorted(
            e.get("name") for e in payload
            if marker in str(e.get("connection_url") or "")
        ),
    }))
asyncio.run(main())
"""
)

_HASH_PROGRAM = (
    _POD_MEMORY_CAP
    + r"""
import hashlib, json, sys
from pathlib import Path
cap_memory()
root = Path(sys.argv[1])
expected = json.loads(sys.stdin.read())
stale = sorted(
    path for path, digest in expected.items()
    if not (root / path).is_file()
    or hashlib.sha256((root / path).read_bytes()).hexdigest() != digest
)
print(json.dumps({"stale": stale}))
"""
)


def in_orchestrator(
    program: str, payload: dict[str, Any], *, timeout: int = 300
) -> dict:
    out = command(
        K
        + ["exec", "-i", ORCHESTRATOR, "-c", ORCHESTRATOR_CONTAINER, "--"]
        + ["python", "-c", program],
        data=json.dumps(payload) + "\n",
        timeout=timeout,
    )
    return json.loads(out.splitlines()[-1])


class Api:
    def __init__(self, username: str, password: str) -> None:
        self.username = username
        self.password = secret(password)

    def call_counting(
        self, method: str, path: str, body: Any = None
    ) -> tuple[int, Any, int]:
        """The answer and how many of this run's secret values its raw body
        held."""
        result = in_orchestrator(
            _API_PROGRAM,
            {
                "username": self.username,
                "password": self.password,
                "token_url": KEYCLOAK_TOKEN_URL,
                "method": method,
                "path": path,
                "body": body,
                "needles": list(_SECRETS),
            },
        )
        text = _scrub(result.get("body") or "")
        try:
            parsed = json.loads(text) if text.strip() else {}
        except json.JSONDecodeError:
            parsed = {"raw": text[:400]}
        return int(result["status"]), parsed, int(result.get("leaks") or 0)

    def call(self, method: str, path: str, body: Any = None) -> tuple[int, Any]:
        status, parsed, _leaks = self.call_counting(method, path, body)
        return status, parsed

    def ok(self, method: str, path: str, body: Any = None) -> Any:
        status, parsed = self.call(method, path, body)
        if status not in (200, 201, 202, 204):
            raise GateError(f"{method} {path} -> HTTP {status}: {str(parsed)[:300]}")
        return parsed


def can_publish_connectors(capabilities: Any) -> bool:
    """An administrator (``grants`` is then null) or the publish grant."""
    if not isinstance(capabilities, dict):
        return False
    if capabilities.get("is_admin"):
        return True
    grants = capabilities.get("grants")
    return isinstance(grants, dict) and grants.get("public_datasources") is True


def expected_bytes() -> dict[str, str]:
    return {
        path: hashlib.sha256((ROOT / path).read_bytes()).hexdigest() for path in SERVED
    }


# ---------------------------------------------------------------------------
# What D3b says (pure, unit-tested)
# ---------------------------------------------------------------------------


def secret_name(datasource_id: str) -> str:
    """``connector-<32 hex of the id>``."""
    return SECRET_PREFIX + str(datasource_id).replace("-", "")


def secret_problems(
    entry: dict[str, Any] | None,
    keys: set[str] | None,
    *,
    scope: tuple[str, str] | None = None,
) -> list[str]:
    """Everything wrong with one connector's secret against D3b.

    ``keys`` is what its driver's slots name for the fixture (``None``: any,
    for a row the gate did not write); an empty set means no secret at all.
    ``scope`` is the scope the Connector must live in, when known.
    """
    if not entry or not entry.get("row"):
        return ["no row"]
    resource = entry.get("resource") or {}
    if not resource or resource.get("deleted"):
        return ["no live Connector"]
    problems: list[str] = []
    own_scope = list(resource.get("scope") or [])
    if scope is not None and own_scope != list(scope):
        problems.append(f"Connector in {own_scope}, expected {list(scope)}")
    refs = resource.get("refs")
    if refs is None:
        problems.append("the Connector names no credentials (written before D3b)")
    elif not resource.get("refs_own"):
        problems.append("a credential is not a reference to its own secret")
    derived = entry.get("derived_keys")
    if keys is not None and derived is not None and set(derived) != keys:
        problems.append(
            f"the deployed mapping derives {derived}, expected {sorted(keys)}"
        )
    want = set(derived) if keys is None and derived is not None else keys or set()
    secrets_ = entry.get("secrets") or []
    elsewhere = [s for s in secrets_ if list(s.get("scope") or []) != own_scope]
    if elsewhere:
        problems.append(
            f"a secret outside the Connector's scope: {elsewhere[0]['scope']}"
        )
    mine = [s for s in secrets_ if list(s.get("scope") or []) == own_scope]
    if not want:
        if mine:
            problems.append("a Connector with nothing secret has a secret")
        if refs:
            problems.append(f"the Connector names keys {refs} but has nothing secret")
        return problems
    if len(mine) != 1:
        return problems + [f"{len(mine)} secrets in the Connector's scope"]
    if set(mine[0].get("keys") or []) != want:
        problems.append(f"secret keys {mine[0].get('keys')}, expected {sorted(want)}")
    if refs is not None and set(refs) != want:
        problems.append(f"the Connector names {refs}, expected {sorted(want)}")
    if not entry.get("rebuilds_row"):
        problems.append("the secret does not rebuild the row's credentials and URL")
    if not entry.get("matches_mapping"):
        problems.append("the secret is not what the mapping derives from the row")
    return problems


def secret_version(entry: dict[str, Any] | None) -> int | None:
    """The version of the secret in the Connector's scope."""
    resource = (entry or {}).get("resource") or {}
    for found in (entry or {}).get("secrets") or []:
        if list(found.get("scope") or []) == list(resource.get("scope") or []):
            return found.get("version")
    return None


# ---------------------------------------------------------------------------
# The gate
# ---------------------------------------------------------------------------


@dataclass
class Report:
    gate_id: str
    results: list[tuple[str, bool, str]] = field(default_factory=list)

    def check(self, name: str, ok: bool, detail: str = "") -> bool:
        detail = _scrub(detail)
        self.results.append((name, bool(ok), detail))
        print(
            f"{'PASS' if ok else 'FAIL'} {name}{': ' + detail if detail else ''}",
            flush=True,
        )
        return bool(ok)

    def note(self, text: str) -> None:
        print(f"NOTE {_scrub(text)}", flush=True)

    @property
    def passed(self) -> bool:
        return bool(self.results) and all(ok for _, ok, _ in self.results)


PLAN = [
    "preflight: Tilt srw ok; every orchestrator pod serves this checkout's D3b "
    "modules; owner, other and stranger accounts approved, owner may publish",
    "fixture: Postgres database + SELECT-only role (its password in every "
    "Postgres URL), a project owned by --user with --other-user as editor "
    "(--stranger-user not a member), one legacy row written straight to the "
    "table, and one connector put back to its D3a shape",
    "backfill: rerun the deployed migrate_stored_connectors: each of this "
    "run's Connectors has exactly one connector-<hex> secret in its scope with "
    "the keys its driver's slots name, named by spec.credentials, rebuilding "
    "the row's credentials and URL; the project KB's secret is in the "
    "project's scope; a rerun changes no secret",
    "write: create writes the secret; a blank edit keeps it; a credentials "
    "edit merges, a generic edit replaces; a URL edit reaches it; delete "
    "removes it; the resource API refuses a connector- secret",
    "shared: public and project-linked Postgres connectors with marked "
    "secrets: --other-user's session is admitted, the deployed delivery path "
    "delivers both from their secrets, the agent logs a connection to each",
    "refused: --stranger-user's session with the linked or a private connector "
    "is refused, and so is the deployed delivery path for a stranger's session",
    "leaks: no gate secret value in this run's revisions or resources, in any "
    "connector or resource API answer, or in orchestrator and agent logs",
    "cleanup: sessions, connectors (their resources and secrets), project, "
    "database and role; residue check by gate id",
]


class ConnectorSecretsGate:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.gate_id = args.gate_id or f"d3b-{secrets.token_hex(5)}"
        self.suffix = self.gate_id.split("-", 1)[1]
        self.report = Report(self.gate_id)
        self.owner = Api(args.user, args.password)
        self.other = Api(args.other_user, args.other_password)
        self.stranger = Api(args.stranger_user, args.stranger_password)
        self.started = (datetime.now(timezone.utc) - timedelta(seconds=30)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        # Everything this run creates, recorded before it is created.
        self.connectors: dict[str, str] = {}  # label -> datasource id
        # Ids this run deleted on purpose: their secrets must be gone.
        self.deleted: dict[str, str] = {}
        self.project: str | None = None
        self.threads: list[str] = []
        self.pg_name = f"d3b_{self.suffix}"
        self.pg_started = False
        self.owner_id = ""
        self.other_id = ""
        self.stranger_id = ""
        self.pg_password = secret(secrets.token_hex(16))
        self.marker = f"d3b-mark-{self.suffix}"
        # Values the resources, APIs and logs must never show.
        self.env_values = {
            name: secret(f"d3b-{name.lower()}-{secrets.token_hex(8)}")
            for name in ("token", "rotated", "a", "a2", "b")
        }

    # -- naming ------------------------------------------------------------
    def name(self, label: str) -> str:
        return f"{self.gate_id} {label}"

    def pg_url(self, password: str | None = None) -> str:
        return (
            f"postgresql://{self.pg_name}:{password or self.pg_password}@"
            f"{PG_HOST}:5432/{self.pg_name}"
        )

    def pods(self, component: str) -> list[dict]:
        listing = json.loads(
            command(
                K
                + ["get", "pods", "-l"]
                + [f"{_SELECTOR},app.kubernetes.io/component={component}"]
                + ["-o", "json"]
            )
        )["items"]
        return [pod for pod in listing if not pod["metadata"].get("deletionTimestamp")]

    def logs(self, component: str, container: str) -> list[str]:
        """Raw log text since the gate started, per pod (never printed)."""
        found: list[str] = []
        for pod in self.pods(component):
            rc, out, _err = run_raw(
                K
                + ["logs", pod["metadata"]["name"], "-c", container]
                + [f"--since-time={self.started}"],
                timeout=120,
            )
            if rc == 0:
                found.append(out)
        return found

    def secrets_of(self, *labels: str) -> dict[str, Any]:
        ids = [self.connectors.get(label) or self.deleted[label] for label in labels]
        found = in_orchestrator(_SECRETS_PROGRAM, {"ids": ids})
        return {label: found.get(i) for label, i in zip(labels, ids)}

    def create_connector(
        self, label: str, body: dict[str, Any], *, api: Api | None = None
    ) -> dict[str, Any]:
        """POST a connector as the owner; its id is recorded before checks."""
        status, parsed = (api or self.owner).call(
            "POST", "/api/datasources", {"name": self.name(label), **body}
        )
        if isinstance(parsed, dict) and parsed.get("id"):
            self.connectors[label] = str(parsed["id"])
        if status not in (200, 201) or label not in self.connectors:
            raise GateError(f"{label} create answered HTTP {status}: {parsed}")
        return parsed

    def queue(self, thread: str) -> tuple[str, int, int] | None:
        row = sql(
            "SELECT state || ' ' || coalesce(input_seq, 0) || ' ' || "
            f"coalesce(consumed_seq, 0) FROM run_queue WHERE unit_id = {lit(thread)}"
        )
        if not row:
            return None
        state, input_seq, consumed_seq = row.split()
        if state == "parked":
            raise GateError("the session unit parked")
        return state, int(input_seq), int(consumed_seq)

    # -- phases ------------------------------------------------------------
    def preflight(self) -> None:
        if shutil.which("tilt"):
            rc, status, _err = run(
                ["tilt", "get", "uiresource", "srw", "-o"]
                + ["jsonpath={.status.updateStatus}"],
                timeout=30,
            )
            if rc or status != "ok":
                raise GateError(f"Tilt srw update status is {status or 'unknown'!r}")
        else:
            self.report.note("tilt not on PATH; rollout state not read")
        problems: list[str] = []
        pods = self.pods("orchestrator")
        if not pods:
            problems.append("no orchestrator pod")
        for pod in pods:
            name = pod["metadata"]["name"]
            statuses = pod.get("status", {}).get("containerStatuses") or []
            if pod.get("status", {}).get("phase") != "Running" or not all(
                status.get("ready") for status in statuses
            ):
                problems.append(f"{name} is not running and ready")
                continue
            found = json.loads(
                command(
                    K
                    + ["exec", "-i", name, "-c", ORCHESTRATOR_CONTAINER, "--"]
                    + ["python", "-c", _HASH_PROGRAM, POD_ROOT],
                    data=json.dumps(expected_bytes()),
                ).splitlines()[-1]
            )
            if found.get("stale"):
                problems.append(f"{name} stale: {found['stale'][:6]}")
        self.report.check(
            "preflight: the orchestrator serves this checkout's D3b code",
            not problems,
            "; ".join(problems)[:600],
        )
        if problems:
            raise GateError("the deployment does not serve this checkout")
        owner = self.owner.ok("GET", "/api/auth/me")["user"]
        other = self.other.ok("GET", "/api/auth/me")["user"]
        stranger = self.stranger.ok("GET", "/api/auth/me")["user"]
        self.owner_id = str(owner["id"])
        self.other_id = str(other["id"])
        self.stranger_id = str(stranger["id"])
        can_publish = can_publish_connectors(
            self.owner.ok("GET", "/api/users/me/capabilities")
        )
        accounts_ok = (
            all(user.get("is_approved") for user in (owner, other, stranger))
            and not other.get("is_admin")
            and not stranger.get("is_admin")
            and len({self.owner_id, self.other_id, self.stranger_id}) == 3
            and can_publish
        )
        self.report.check(
            "preflight: three approved accounts, the owner may publish, the "
            "other two are not administrators",
            bool(accounts_ok),
            f"owner admin={owner.get('is_admin')} publish={can_publish}, "
            f"other admin={other.get('is_admin')}, "
            f"stranger admin={stranger.get('is_admin')}",
        )
        if not accounts_ok:
            raise GateError("the three accounts cannot run this gate")

    def fixture(self) -> None:
        self.pg_started = True
        sql_script(
            f"CREATE ROLE {self.pg_name} LOGIN PASSWORD '{self.pg_password}' "
            "NOSUPERUSER NOCREATEDB NOCREATEROLE CONNECTION LIMIT 20;\n"
            f"CREATE DATABASE {self.pg_name} OWNER srw;\n"
            f"REVOKE ALL ON DATABASE {self.pg_name} FROM PUBLIC;\n"
            f"GRANT CONNECT ON DATABASE {self.pg_name} TO {self.pg_name};\n"
        )
        sql_script(
            "CREATE TABLE d3b_marker (note text NOT NULL);\n"
            f"INSERT INTO d3b_marker VALUES ('{self.marker}');\n"
            "REVOKE CREATE ON SCHEMA public FROM PUBLIC;\n"
            f"GRANT USAGE ON SCHEMA public TO {self.pg_name};\n"
            f"GRANT SELECT ON d3b_marker TO {self.pg_name};\n",
            database=self.pg_name,
        )
        created = self.owner.ok(
            "POST",
            "/api/projects",
            {
                "name": self.name("project"),
                "description": "D3b connector secrets gate (disposable)",
                "user_id": self.owner_id,
            },
        )
        self.project = str(created["id"])
        self.owner.ok(
            "POST",
            f"/api/projects/{self.project}/members",
            {"user_id": self.other_id, "role": "editor"},
        )
        # As an orchestrator without the write-through would write it. Its id
        # is chosen here, so it is recorded before the row exists; the
        # password reaches psql on stdin only.
        legacy_id = sql("SELECT gen_random_uuid()")
        if not _UUID_RE.fullmatch(legacy_id):
            raise GateError(f"no fresh id: {legacy_id!r}")
        self.connectors["legacy"] = legacy_id
        sql_script(
            "INSERT INTO datasources (id, name, type, connection_url, created_by) "
            f"VALUES ({lit(legacy_id)}, {lit(self.name('legacy'))}, 'postgresql', "
            f"{lit(self.pg_url())}, {lit(self.owner_id)});\n"
        )
        # Written through, then put back to how D3a left it.
        self.create_connector(
            "pre-d3b",
            {
                "type": "credentials",
                "credentials": {
                    "env_vars": {"D3B_GATE_TOKEN": self.env_values["token"]}
                },
            },
        )
        pre = self.connectors["pre-d3b"]
        sql(
            "UPDATE srw_resources SET document = document #- '{spec,credentials}' "
            f"WHERE id = {lit(pre)}"
        )
        sql(f"DELETE FROM srw_resource_secrets WHERE name = {lit(secret_name(pre))}")
        print(
            f"fixture: database {self.pg_name}, project {self.project}, "
            f"legacy row {legacy_id}, pre-D3b connector {pre}",
            flush=True,
        )

    def backfill(self) -> None:
        counts = in_orchestrator(_BACKFILL_PROGRAM, {})
        if counts.get("deferred"):
            self.report.note(f"the rerun deferred {counts['deferred']} rows")
        found = self.secrets_of("legacy", "pre-d3b")
        owner = ("Account", self.owner_id)
        problems = [
            f"legacy: {problem}"
            for problem in secret_problems(found["legacy"], {URL_KEY}, scope=owner)
        ] + [
            f"pre-d3b: {problem}"
            for problem in secret_problems(
                found["pre-d3b"], {"env.D3B_GATE_TOKEN", SHAPE_KEY}, scope=owner
            )
        ]
        self.report.check(
            "backfill: the legacy row and the D3a-shaped Connector each have one "
            "connector-<hex> secret in the owner's Account with the slot keys, "
            "named by the resource, rebuilding the row",
            not problems,
            "; ".join(problems[:5]) or json.dumps(counts),
        )
        native = sql(
            "SELECT id FROM datasources WHERE type = 'kb' AND "
            f"config->>'native_project_id' = {lit(self.project)} "
            "ORDER BY created_at LIMIT 1"
        )
        if _UUID_RE.fullmatch(native or ""):
            entry = in_orchestrator(_SECRETS_PROGRAM, {"ids": [native]})[native]
            problems = secret_problems(entry, None, scope=("Project", self.project))
            self.report.check(
                "backfill: the project's own knowledge base keeps its secret in "
                "the project's scope",
                not problems,
                "; ".join(problems[:5]),
            )
        else:
            self.report.note("the project has no knowledge base (is Gitea up?)")
        versions = {
            label: secret_version(entry)
            for label, entry in self.secrets_of("legacy", "pre-d3b").items()
        }
        again = in_orchestrator(_BACKFILL_PROGRAM, {})
        after = {
            label: secret_version(entry)
            for label, entry in self.secrets_of("legacy", "pre-d3b").items()
        }
        self.report.check(
            "backfill: a rerun changes no secret",
            versions == after and None not in versions.values(),
            f"{versions} -> {after}; {json.dumps(again)}",
        )

    def write_through(self) -> None:
        owner = ("Account", self.owner_id)
        self.create_connector(
            "orders", {"type": "postgresql", "connection_url": self.pg_url()}
        )
        orders = self.secrets_of("orders")["orders"]
        problems = secret_problems(orders, {URL_KEY}, scope=owner)
        self.report.check(
            "write: a create writes the Connector's secret (the full URL, its "
            "password included) in the owner's Account",
            not problems and secret_version(orders) == 1,
            "; ".join(problems[:5]) or f"version {secret_version(orders)}",
        )
        vendor_keys = {"env.D3B_VENDOR_USER", "env.D3B_VENDOR_TOKEN", SHAPE_KEY}
        self.create_connector(
            "vendor",
            {
                "type": "credentials",
                "credentials": {
                    "env_vars": {
                        "D3B_VENDOR_USER": f"user-{self.suffix}",
                        "D3B_VENDOR_TOKEN": self.env_values["token"],
                    }
                },
            },
        )
        vendor = self.connectors["vendor"]
        before = self.secrets_of("vendor")["vendor"]
        self.owner.ok("PUT", f"/api/datasources/{vendor}", {"description": "edited"})
        self.owner.ok(
            "PUT", f"/api/datasources/{vendor}", {"credentials": {}, "cli_hint": "x"}
        )
        kept = self.secrets_of("vendor")["vendor"]
        problems = secret_problems(kept, vendor_keys, scope=owner)
        self.report.check(
            "write: an edit without credentials, or with an empty object, keeps "
            "the secret",
            not problems and secret_version(kept) == secret_version(before) == 1,
            "; ".join(problems[:5])
            or f"versions {secret_version(before)} -> {secret_version(kept)}",
        )
        self.owner.ok(
            "PUT",
            f"/api/datasources/{vendor}",
            {
                "credentials": {
                    "env_vars": {
                        "D3B_VENDOR_TOKEN": self.env_values["rotated"],
                        "D3B_VENDOR_MFA": "1",
                    }
                }
            },
        )
        merged = self.secrets_of("vendor")["vendor"]
        problems = secret_problems(
            merged, vendor_keys | {"env.D3B_VENDOR_MFA"}, scope=owner
        )
        self.report.check(
            "write: a credentials edit merges (the variable it does not name stays)",
            not problems and secret_version(merged) == 2,
            "; ".join(problems[:5]) or f"version {secret_version(merged)}",
        )
        self.create_connector(
            "env",
            {
                "type": "generic",
                "credentials": {
                    "env_vars": {
                        "D3B_A": self.env_values["a"],
                        "D3B_B": self.env_values["b"],
                    }
                },
            },
        )
        self.owner.ok(
            "PUT",
            f"/api/datasources/{self.connectors['env']}",
            {"credentials": {"env_vars": {"D3B_A": self.env_values["a2"]}}},
        )
        replaced = self.secrets_of("env")["env"]
        problems = secret_problems(replaced, {"env.D3B_A", SHAPE_KEY}, scope=owner)
        self.report.check(
            "write: a generic edit replaces (the variable it does not name goes)",
            not problems and secret_version(replaced) == 2,
            "; ".join(problems[:5]) or f"version {secret_version(replaced)}",
        )
        rotated = secret(secrets.token_hex(16))
        sql_script(f"ALTER ROLE {self.pg_name} PASSWORD '{rotated}';\n")
        self.pg_password = rotated
        self.owner.ok(
            "PUT",
            f"/api/datasources/{self.connectors['orders']}",
            {"connection_url": self.pg_url()},
        )
        moved = self.secrets_of("orders")["orders"]
        problems = secret_problems(moved, {URL_KEY}, scope=owner)
        self.report.check(
            "write: a URL edit reaches the secret",
            not problems and secret_version(moved) == 2,
            "; ".join(problems[:5]) or f"version {secret_version(moved)}",
        )
        status, body = self.owner.call(
            "PUT",
            f"/api/resource-secrets/{secret_name(vendor)}",
            {
                "scope": {"kind": "Account", "name": "me"},
                "values": {"env.D3B_VENDOR_TOKEN": "overwritten"},
                "expected_version": 2,
            },
        )
        untouched = self.secrets_of("vendor")["vendor"]
        self.report.check(
            "write: the resource API refuses to write a connector- secret",
            status == 409
            and body.get("detail") == CONNECTOR_SECRET_DETAIL
            and secret_version(untouched) == 2,
            f"HTTP {status}: {str(body.get('detail'))[:120]}",
        )
        orders_id = self.connectors.pop("orders")
        self.deleted["orders"] = orders_id
        self.owner.ok("DELETE", f"/api/datasources/{orders_id}")
        gone = self.secrets_of("orders")["orders"]
        self.report.check(
            "write: a delete removes the secret with the row and retires the Connector",
            gone is not None
            and not gone.get("row")
            and (gone.get("resource") or {}).get("deleted") is True
            and not gone.get("secrets"),
            json.dumps(
                {
                    "row": (gone or {}).get("row"),
                    "secrets": len((gone or {}).get("secrets") or []),
                }
            ),
        )

    def shared(self) -> None:
        self.create_connector(
            "public",
            {
                "type": "postgresql",
                "connection_url": self.pg_url(),
                "scope_mode": "all",
                "is_global": True,
                "read_only": True,
            },
        )
        self.create_connector(
            "linked",
            {
                "type": "postgresql",
                "connection_url": self.pg_url(),
                "scope_mode": "projects",
                "project_ids": [self.project],
            },
        )
        self.create_connector(
            "private", {"type": "postgresql", "connection_url": self.pg_url()}
        )
        ids = [self.connectors["public"], self.connectors["linked"]]
        marked = in_orchestrator(_MARK_PROGRAM, {"ids": ids, "marker": self.marker})
        if sorted(marked.get("marked") or []) != sorted(ids):
            raise GateError(f"could not mark the shared secrets: {marked}")
        status, created = self.other.call(
            "POST",
            "/api/persistent/threads",
            {
                "title": f"D3b connector secrets gate {self.gate_id}",
                "permission_mode": "autonomous",
                "project_id": self.project,
                "datasource_ids": ids,
                "config_override": {"workspace": {"backend": "sandbox"}},
                "model": self.args.model,
            },
        )
        thread = None
        if isinstance(created, dict) and (
            created.get("thread_id") or created.get("id")
        ):
            thread = str(created.get("thread_id") or created["id"])
            self.threads.append(thread)
        self.report.check(
            "shared: the other account's session selecting the public and the "
            "project-linked connector is admitted",
            status in (200, 201) and bool(thread),
            f"HTTP {status}: {str(created)[:160]}" if status not in (200, 201) else "",
        )
        if not thread:
            return
        delivered = in_orchestrator(
            _DELIVERY_PROGRAM,
            {
                "thread_id": thread,
                "datasource_ids": ids,
                "project_ids": [self.project],
                "marker": self.marker,
            },
        )
        names = sorted(self.name(label) for label in ("public", "linked"))
        self.report.check(
            "shared: the deployed delivery path delivers both to the other "
            "account's session from their secrets",
            delivered.get("status") == 200
            and delivered.get("delivered") == names
            and delivered.get("from_secret") == names,
            json.dumps(delivered),
        )
        previous = (self.queue(thread) or ("", 0, 0))[1]
        self.other.ok(
            "POST",
            f"/api/persistent/threads/{thread}/input",
            {"content": "Reply with the single word ready."},
        )

        def answered() -> bool:
            current = self.queue(thread)
            return bool(
                current
                and current[0] == "done"
                and current[1] > previous
                and current[1] == current[2]
            )

        wait_for("turn answered", answered, timeout=self.args.turn_timeout)
        logs = "\n".join(self.logs("agent-stateless", "agent"))
        connected = [
            name
            for name in names
            if f"Connected to postgresql datasource: {name}" in logs
        ]
        self.report.check(
            "shared: the agent logs a connection to the public and the "
            "project-linked connector, through their secrets' URLs",
            connected == names,
            f"connected {connected}",
        )
        seen = sql(
            "SELECT count(*) FROM pg_stat_activity WHERE application_name = "
            f"{lit(self.marker)}"
        )
        self.report.note(f"{seen} open connections carry the secret's mark")

    def refused(self) -> None:
        wrong: list[str] = []
        for label, body in (
            ("linked", {"datasource_ids": [self.connectors["linked"]]}),
            ("private", {"datasource_ids": [self.connectors["private"]]}),
        ):
            status, created = self.stranger.call(
                "POST",
                "/api/persistent/threads",
                {
                    "title": f"D3b connector secrets gate {self.gate_id} refused",
                    "permission_mode": "autonomous",
                    "config_override": {"workspace": {"backend": "sandbox"}},
                    "model": self.args.model,
                    **body,
                },
            )
            if isinstance(created, dict) and (
                created.get("thread_id") or created.get("id")
            ):
                self.threads.append(str(created.get("thread_id") or created["id"]))
            if status != 403 or created.get("detail") != UNAVAILABLE_DETAIL:
                wrong.append(f"{label}: HTTP {status} {str(created)[:100]}")
        self.report.check(
            "refused: the stranger's session selecting the linked or a private "
            "connector is refused at creation",
            not wrong,
            "; ".join(wrong),
        )
        delivered = in_orchestrator(
            _DELIVERY_PROGRAM,
            {
                "thread": {
                    "id": sql("SELECT gen_random_uuid()"),
                    "user_id": self.stranger_id,
                    "metadata": {},
                },
                "datasource_ids": [self.connectors["linked"]],
                "project_ids": [self.project],
                "marker": self.marker,
            },
        )
        self.report.check(
            "refused: the deployed delivery path refuses a stranger's session "
            "naming the linked connector",
            delivered.get("status") == 403,
            json.dumps(delivered),
        )

    def leaks(self) -> None:
        ids = list(self.connectors.values()) + list(self.deleted.values())
        id_list = ", ".join(lit(i) for i in ids)
        stored = sql_raw(
            "SELECT coalesce(string_agg(document::text, ''), '') FROM "
            f"srw_resource_revisions WHERE resource_id IN ({id_list})"
        ) + sql_raw(
            "SELECT coalesce(string_agg(document::text || resolved::text, ''), '') "
            f"FROM srw_resources WHERE id IN ({id_list})"
        )
        self.report.check(
            "leaks: no secret value in this run's resource revisions or resources",
            bool(stored.strip()) and leak_count(stored) == 0,
            f"{leak_count(stored)} occurrences",
        )
        calls: list[tuple[Api, str, str]] = [
            (self.owner, "GET", "/api/datasources"),
            (self.owner, "GET", "/api/datasources/drivers"),
            (self.owner, "GET", "/api/resources?kind=Connector"),
            (self.other, "GET", "/api/datasources"),
        ]
        for datasource_id in self.connectors.values():
            calls += [
                (self.owner, "GET", f"/api/datasources/{datasource_id}"),
                (self.owner, "GET", f"/api/resources/{datasource_id}"),
            ]
        for label in ("public", "linked"):
            calls.append(
                (self.owner, "POST", f"/api/datasources/{self.connectors[label]}/test")
            )
        leaked = []
        for api, method, path in calls:
            status, _body, count = api.call_counting(method, path)
            if count:
                leaked.append(f"{method} {path} ({status}): {count}")
        self.report.check(
            f"leaks: no secret value in {len(calls)} connector, resource, Test and "
            "matrix API answers",
            not leaked,
            "; ".join(leaked[:5]),
        )
        logs = self.logs("orchestrator", ORCHESTRATOR_CONTAINER) + self.logs(
            "agent-stateless", "agent"
        )
        self.report.check(
            "leaks: no secret value in the orchestrator's or the stateless agents' "
            "logs since the gate started",
            bool(logs) and sum(leak_count(text) for text in logs) == 0,
            f"{sum(leak_count(text) for text in logs)} occurrences in {len(logs)} logs",
        )

    def delete_thread(self, thread: str, api: Api) -> bool:
        def gone() -> bool:
            status, _body = api.call(
                "DELETE",
                f"/api/persistent/threads/{thread}?force=true&permanent=true",
            )
            if status == 404:
                return True
            return sql(f"SELECT count(*) FROM threads WHERE id = {lit(thread)}") == "0"

        try:
            wait_for("session deleted", gone, timeout=300, interval=5)
        except GateError:
            return False
        return True

    def titled_threads(self) -> list[str]:
        rows_ = sql(
            "SELECT id FROM threads WHERE "
            f"position({lit(self.gate_id)} in coalesce(title, '')) > 0"
        )
        return [row for row in rows_.splitlines() if _UUID_RE.fullmatch(row)]

    def thread_owner(self, thread: str) -> Api:
        user = sql(f"SELECT user_id FROM threads WHERE id = {lit(thread)}")
        return {self.stranger_id: self.stranger, self.owner_id: self.owner}.get(
            user, self.other
        )

    def cleanup(self) -> list[str]:
        problems: list[str] = []

        def step(label: str, action: Callable[[], Any]) -> None:
            try:
                if action() is False:
                    problems.append(label)
            except GateError as exc:
                problems.append(f"{label} ({exc})")

        for thread in dict.fromkeys(self.threads + self.titled_threads()):
            step(
                f"delete session {thread}",
                lambda t=thread: self.delete_thread(t, self.thread_owner(t)),
            )
        for label, datasource_id in list(self.connectors.items()):

            def delete(datasource_id=datasource_id) -> bool:
                status, _body = self.owner.call(
                    "DELETE", f"/api/datasources/{datasource_id}"
                )
                return status in (200, 204, 404)

            step(f"delete connector {label}", delete)
        if self.project:

            def project_deleted() -> bool:
                status, _body = self.owner.call(
                    "DELETE", f"/api/projects/{self.project}"
                )
                return status in (200, 204, 404)

            step(
                "delete project",
                lambda: bool(
                    wait_for(
                        "project deleted", project_deleted, timeout=180, interval=10
                    )
                ),
            )
        if self.pg_started:
            step(
                "drop database and role",
                lambda: sql_script(
                    f"DROP DATABASE IF EXISTS {self.pg_name} WITH (FORCE);\n"
                    f"DROP ROLE IF EXISTS {self.pg_name};\n"
                ),
            )
        for problem in problems:
            print(f"cleanup: {problem} failed", flush=True)
        return problems

    def residue(self) -> list[str]:
        """What this run created and cleanup did not remove."""
        left: list[str] = []
        titled = self.titled_threads()
        if titled:
            left.append(f"sessions titled with the gate id: {titled}")
        prefix = self.gate_id.replace("_", "\\_") + " %"
        count = sql(f"SELECT count(*) FROM datasources WHERE name LIKE {lit(prefix)}")
        if count != "0":
            left.append(f"{count} connectors")
        live = sql(
            "SELECT count(*) FROM srw_resources WHERE kind = 'Connector' AND "
            f"deleted_at IS NULL AND name LIKE {lit(self.gate_id + '-%')}"
        )
        if live != "0":
            left.append(f"{live} live Connector resources")
        names = [
            secret_name(i)
            for i in list(self.connectors.values()) + list(self.deleted.values())
        ]
        if names:
            kept = sql(
                "SELECT count(*) FROM srw_resource_secrets WHERE name IN ("
                + ", ".join(lit(name) for name in names)
                + ")"
            )
            if kept != "0":
                left.append(f"{kept} connector secrets")
        if self.project and (
            sql(f"SELECT count(*) FROM projects WHERE id = {lit(self.project)}") != "0"
        ):
            left.append(f"project {self.project}")
        for thread in dict.fromkeys(self.threads + titled):
            try:
                wait_for(
                    f"pods of {thread} gone",
                    lambda thread=thread: not json.loads(
                        command(
                            K
                            + ["get", "pods", "-l", f"srw/thread-id={thread}"]
                            + ["-o", "json"]
                        )
                    )["items"],
                    timeout=180,
                    interval=10,
                )
            except GateError:
                left.append(f"pods of {thread}")
        if (
            self.pg_started
            and sql(
                "SELECT (SELECT count(*) FROM pg_database WHERE datname = "
                f"{lit(self.pg_name)}) + (SELECT count(*) FROM pg_roles WHERE "
                f"rolname = {lit(self.pg_name)})"
            )
            != "0"
        ):
            left.append(f"database or role {self.pg_name}")
        return left

    # -- run ---------------------------------------------------------------
    def run(self) -> int:
        try:
            self.preflight()
            self.fixture()
            self.backfill()
            self.write_through()
            self.shared()
            self.refused()
            self.leaks()
        except GateError as exc:
            self.report.check("gate infrastructure", False, str(exc))
        finally:
            if self.args.keep:
                print(
                    "kept: "
                    + json.dumps(
                        {
                            "connectors": self.connectors,
                            "project": self.project,
                            "threads": self.threads,
                            "database": self.pg_name if self.pg_started else None,
                        }
                    )
                )
            else:
                self.cleanup()
                try:
                    left = self.residue()
                except GateError as exc:
                    left = [f"residue check failed: {exc}"]
                self.report.check(
                    "cleanup: nothing this run created is left",
                    not left,
                    "; ".join(left),
                )
        verdict = "PASS" if self.report.passed else "FAIL"
        failed = [name for name, ok, _ in self.report.results if not ok]
        print(
            f"{verdict} {self.gate_id}: {len(self.report.results)} checks, "
            f"{len(failed)} failed {failed if failed else ''}".rstrip()
        )
        return 0 if self.report.passed else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--context", default=LOCAL_CONTEXT)
    parser.add_argument("--namespace", default=LOCAL_NAMESPACE)
    parser.add_argument("--run", action="store_true")
    parser.add_argument("--confirm")
    parser.add_argument("--gate-id")
    parser.add_argument("--user", default="test")
    parser.add_argument("--password", default="srw-k3d-dev-test")
    parser.add_argument("--other-user", default="dev-user-1")
    parser.add_argument("--other-password", default="srw-k3d-dev-usr1")
    parser.add_argument("--stranger-user", default="dev-user-2")
    parser.add_argument("--stranger-password", default="srw-k3d-dev-usr2")
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--turn-timeout", type=int, default=420)
    parser.add_argument("--keep", action="store_true", help="skip cleanup")
    return parser


def validate(args: argparse.Namespace) -> None:
    if args.context != LOCAL_CONTEXT or args.namespace != LOCAL_NAMESPACE:
        raise SafetyError("this gate is restricted to k3d-srw/srw")
    if args.run and args.confirm != LOCAL_CONFIRMATION:
        raise SafetyError(f"--run requires --confirm {LOCAL_CONFIRMATION}")
    if not args.run and args.confirm is not None:
        raise SafetyError("--confirm is accepted only with --run")
    if args.gate_id is not None and not _GATE_ID_RE.fullmatch(args.gate_id):
        raise SafetyError("--gate-id must be d3b- followed by 10 hex digits")
    if not _MODEL_RE.fullmatch(args.model):
        raise SafetyError("model id is malformed")
    users = (args.user, args.other_user, args.stranger_user)
    for user in users:
        if not _USER_RE.fullmatch(user):
            raise SafetyError("user name is malformed")
    if len(set(users)) != 3:
        raise SafetyError("--user, --other-user and --stranger-user must differ")
    if not 60 <= args.turn_timeout <= 1800:
        raise SafetyError("--turn-timeout must be between 60 and 1800 seconds")


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        validate(args)
    except SafetyError as exc:
        print(f"refused: {exc}", file=sys.stderr)
        return 2
    if not args.run:
        print("plan (dry run; pass --run --confirm LOCAL-K3D-DISPOSABLE to execute):")
        for step in PLAN:
            print(f"  - {step}")
        return 0
    return ConnectorSecretsGate(args).run()


if __name__ == "__main__":
    raise SystemExit(main())
