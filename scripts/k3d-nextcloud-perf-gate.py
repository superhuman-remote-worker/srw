#!/usr/bin/env python3
"""Local k3d performance gate for the bundled Nextcloud (perf plan WP10).

Design: knowledge-base/knowledge/features/bundled_nextcloud_performance.md,
WP10. Every Nextcloud work package is accepted on these numbers; run it
before and after each one.

  request   the cost of one Nextcloud request per identity, from the
            orchestrator pod through the production backend: agent-service
            (re-ensuring a session folder that is there) and admin (a user
            search by email), each timed --requests times; median and p95,
            and how many Nextcloud requests each call sent
  create    sandbox sessions created through the REST API for the k3d
            ``test`` user, with its session outbox attached: the first with the user's
            Nextcloud identity cache empty (a miss), then --sessions more
            (hits). Per phase: the Nextcloud requests the orchestrator sent
            per create (access log, by identity and verb), the create's wall
            time (p50/p95) and the orchestrator's ``thread-create timing:``
            steps
  tokens    rows ``oc_authtoken`` gained over the run, per Nextcloud request
            the orchestrator sent

The test user needs a Nextcloud account the identity lookup finds (by email,
or on k3d by display name: Nextcloud refuses ``admin@localhost`` as an email).
Unless one exists, the gate creates a disposable account (``srw-perf-<id>``,
display name = the test user's, no password anyone knows) and deletes it
afterwards. The user's ``users.cloud_identity`` is restored to
its value before the run, and every session the run created is deleted.

Run with the repository venv on the k3d-srw cluster, alone and with the
cluster otherwise idle: Nextcloud requests in the window are counted. Uses
the disposable k3d test account; set SRW_K3D_TEST_PASSWORD if its password
was changed. Prints measurements before any expectation is checked; the exit
status is 0 only if every requested expectation passes.

  .venv/bin/python scripts/k3d-nextcloud-perf-gate.py                    # plan
  .venv/bin/python scripts/k3d-nextcloud-perf-gate.py --run \\
      --confirm LOCAL-K3D-DISPOSABLE [--sessions 8] [--requests 10] \\
      [--expect-create-requests 2] [--expect-agent-ms 150] \\
      [--expect-token-rows-per-request 0.1] [--json out.json]
"""

from __future__ import annotations

import argparse
import json
import os
import re
import secrets
import shlex
import statistics
import subprocess
import sys
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Any

LOCAL_CONTEXT = "k3d-srw"
LOCAL_NAMESPACE = "srw"
LOCAL_CONFIRMATION = "LOCAL-K3D-DISPOSABLE"
K = ["kubectl", f"--context={LOCAL_CONTEXT}", "-n", LOCAL_NAMESPACE]
ORCHESTRATOR = "deploy/srw-orchestrator"
NEXTCLOUD = "deploy/srw-nextcloud"
TEST_USERNAME = "test"
TEST_EMAIL = "admin@localhost"
#: The orchestrator's Nextcloud clients name themselves with this prefix.
SRW_USER_AGENT = "SuperhumanRemoteWorker/"

_ACCESS_LINE = re.compile(
    r'^\S+ \S+ (?P<user>\S+) \[(?P<when>[^\]]+)\] "(?P<method>[A-Z]+) (?P<path>\S+)'
    r'[^"]*" (?P<status>\d{3}) \S+ "[^"]*" "(?P<agent>[^"]*)"'
)

_REQUEST_PROBE = r"""
import asyncio, json, sys, time
from orchestrator.database import PostgresDB
from orchestrator.services.cloud import SessionFolderHandle, build_backend
from orchestrator.services.cloud.service_credentials import (
    configure_service_credential_store,
)

instance, folder, email, name = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
count = int(sys.argv[5])


async def main():
    # agent-service's stored app password, as the orchestrator adopts it.
    db = PostgresDB(min_connections=1, max_connections=2)
    await db.connect()
    configure_service_credential_store(db)
    backend = build_backend("nextcloud")
    backend.bind_backend_instance(instance)
    if not await backend.ensure_initialized():
        await db.close()
        raise SystemExit("the Nextcloud backend did not initialize")
    result = {"identity": None, "agent": [], "admin": [], "agent_secret": None}
    try:
        if count == 0:
            result["identity"] = await backend.resolve_user_identity(email, name)
            return result
        # Untimed: adopts (or mints) the app password, then creates the folder.
        await backend.ensure_session_folder(session_id=folder)
        result["agent_secret"] = getattr(backend, "_agent_secret_kind", "password")
        for _ in range(count):
            started = time.perf_counter()
            await backend.ensure_session_folder(session_id=folder)
            result["agent"].append(time.perf_counter() - started)
        for _ in range(count):
            started = time.perf_counter()
            await backend.resolve_user_identity(email, name)
            result["admin"].append(time.perf_counter() - started)
    finally:
        if count:
            await backend.delete_session_folder(
                SessionFolderHandle(backend="nextcloud", native_id=f"sessions/{folder}")
            )
        await backend.close()
        await db.close()
    return result


print("RESULT " + json.dumps(asyncio.run(main())))
"""

_TOKEN_COUNT_PHP = """<?php
require '/var/www/html/lib/base.php';
$db = \\OCP\\Server::get(\\OCP\\IDBConnection::class);
echo $db->executeQuery('SELECT COUNT(*) FROM `*PREFIX*authtoken`')->fetchOne();
"""


class GateError(RuntimeError):
    pass


def command(args: list[str], *, data: str | None = None) -> str:
    result = subprocess.run(args, input=data, text=True, capture_output=True)
    if result.returncode:
        # Commands can carry an in-flight credential: never print argv.
        tail = (result.stderr or "").strip().splitlines()[-3:]
        tail += (result.stdout or "").strip().splitlines()[-3:]
        raise GateError(f"command failed (exit {result.returncode}): {tail}")
    return result.stdout.strip()


def sql(query: str) -> str:
    return command(
        K
        + ["exec", "-i", "srw-postgres-0", "--"]
        + ["psql", "-U", "srw", "-d", "srw", "-v", "ON_ERROR_STOP=1", "-tA"],
        data=query,
    )


def lit(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def occ(*args: str) -> str:
    line = "php occ " + " ".join(shlex.quote(arg) for arg in args)
    return command(
        K
        + ["exec", NEXTCLOUD, "-c", "nextcloud", "--"]
        + ["su", "-s", "/bin/sh", "www-data", "-c", line]
    )


def token_rows() -> int:
    out = command(
        K
        + ["exec", "-i", NEXTCLOUD, "-c", "nextcloud", "--"]
        + ["su", "-s", "/bin/sh", "www-data", "-c", "cd /var/www/html && php"],
        data=_TOKEN_COUNT_PHP,
    )
    return int(out.strip().splitlines()[-1])


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def since_arg(when: datetime) -> str:
    return (when - timedelta(seconds=2)).strftime("%Y-%m-%dT%H:%M:%SZ")


def access_lines(since: datetime) -> list[dict[str, Any]]:
    raw = command(
        K + ["logs", NEXTCLOUD, "-c", "nextcloud", f"--since-time={since_arg(since)}"]
    )
    lines = []
    for line in raw.splitlines():
        match = _ACCESS_LINE.match(line)
        if not match:
            continue
        entry = match.groupdict()
        entry["at"] = datetime.strptime(entry["when"], "%d/%b/%Y:%H:%M:%S %z")
        lines.append(entry)
    return lines


def in_window(
    lines: list[dict[str, Any]], start: datetime, end: datetime
) -> list[dict[str, Any]]:
    # The access log has second resolution: phases are separated by sleeps.
    low = start.replace(microsecond=0)
    high = end.replace(microsecond=0) + timedelta(seconds=1)
    return [
        line
        for line in lines
        if low <= line["at"] < high and SRW_USER_AGENT in line["agent"]
    ]


def by_identity(lines: list[dict[str, Any]]) -> dict[str, int]:
    counts = Counter(
        f"{line['user']} {line['method']} {line['status']}" for line in lines
    )
    return dict(sorted(counts.items()))


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    if not ordered:
        return float("nan")
    index = min(len(ordered) - 1, max(0, round(fraction * (len(ordered) - 1))))
    return ordered[index]


def summary_ms(values: list[float]) -> dict[str, float]:
    return {
        "n": len(values),
        "p50_ms": round(statistics.median(values) * 1000, 1) if values else None,
        "p95_ms": round(percentile(values, 0.95) * 1000, 1) if values else None,
        "max_ms": round(max(values) * 1000, 1) if values else None,
    }


class Gate:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.gate_id = args.gate_id or f"perf-{secrets.token_hex(5)}"
        self.token: str | None = None
        self.created_threads: list[str] = []
        self.created_nc_user: str | None = None
        self.saved_identity: str | None = None
        self.user_id: str | None = None
        self.display_name: str | None = None
        self.outbox: str | None = None
        self.instance: str | None = None
        self.report: dict[str, Any] = {"gate_id": self.gate_id}
        self.failures: list[str] = []

    # ------------------------------------------------------------ plumbing
    def login(self) -> None:
        password = os.environ.get("SRW_K3D_TEST_PASSWORD", "srw-k3d-dev-test")
        out = command(
            K
            + ["exec", "-i", ORCHESTRATOR, "-c", "orchestrator", "--"]
            + ["curl", "-fsS", "-X", "POST"]
            + ["http://srw-keycloak:8080/realms/srw/protocol/openid-connect/token"]
            + ["-d", "grant_type=password", "-d", "client_id=admin-cli"]
            + ["-d", f"username={TEST_USERNAME}", "-d", "scope=openid"]
            + ["--data-urlencode", "password@-"],
            data=password,
        )
        self.token = json.loads(out)["id_token"]

    def api(
        self, method: str, path: str, body: dict[str, Any] | None = None
    ) -> tuple[dict[str, Any], float]:
        assert self.token
        out = command(
            K
            + ["exec", "-i", ORCHESTRATOR, "-c", "orchestrator", "--"]
            + ["curl", "-fsS", "-X", method, f"http://localhost:8085{path}"]
            + ["-H", f"Authorization: Bearer {self.token}"]
            + ["-H", "Content-Type: application/json"]
            + ["--data-binary", "@-", "-w", "\n%{time_total}"],
            data=json.dumps(body or {}),
        )
        payload, _, seconds = out.rpartition("\n")
        return (json.loads(payload) if payload.strip() else {}), float(seconds)

    def probe(self, *, count: int, folder: str = "-") -> dict[str, Any]:
        out = command(
            K
            + ["exec", "-i", ORCHESTRATOR, "-c", "orchestrator", "--"]
            + ["python", "-", self.instance or "", folder, TEST_EMAIL]
            + [self.display_name or "", str(count)],
            data=_REQUEST_PROBE,
        )
        for line in out.splitlines():
            if line.startswith("RESULT "):
                return json.loads(line[len("RESULT ") :])
        raise GateError("the request probe printed no result")

    # --------------------------------------------------------------- steps
    def preflight(self) -> None:
        current = command(["kubectl", "config", "get-contexts", "-o", "name"])
        if LOCAL_CONTEXT not in current.split():
            raise GateError(f"kubectl context {LOCAL_CONTEXT} is missing")
        self.user_id = sql(f"SELECT id FROM users WHERE email = {lit(TEST_EMAIL)}")
        if not self.user_id:
            raise GateError(f"no SRW user {TEST_EMAIL}")
        self.display_name = sql(
            f"SELECT lower(display_name) FROM users WHERE id = {lit(self.user_id)}"
        )
        outbox = self.outbox = sql(
            "SELECT id FROM datasources WHERE managed_key = "
            + lit(f"cloud-outbox:{self.user_id}")
        )
        if not outbox:
            raise GateError(
                "the test user has no session outbox connector, so a session "
                "gets no Nextcloud folder and there is nothing to measure"
            )
        self.instance = sql("SELECT backend_instance_id FROM main_cloud_active_backend")
        if not self.instance:
            raise GateError("no active main cloud")
        self.saved_identity = sql(
            f"SELECT COALESCE(cloud_identity, '{{}}')::text FROM users "
            f"WHERE id = {lit(self.user_id)}"
        )
        print(f"PASS preflight (gate {self.gate_id})", flush=True)

    def ensure_nc_account(self) -> None:
        found = self.probe(count=0).get("identity")
        if found:
            print(f"PASS the test user has Nextcloud account {found}", flush=True)
            self.report["nc_account"] = {"uid": found, "created": False}
            return
        uid = f"srw-perf-{self.gate_id.split('-', 1)[-1]}"
        # Nextcloud refuses ``admin@localhost`` as an email address (no
        # TLD), so on k3d the lookup finds the account by display name, as
        # it would find the one user_oidc creates at the user's first login.
        # Recorded first: occ can create the account and still fail after.
        self.created_nc_user = uid
        occ(
            "user:add",
            "--generate-password",
            f"--display-name={self.display_name}",
            uid,
        )
        self.report["nc_account"] = {"uid": uid, "created": True}
        print(f"PASS created disposable Nextcloud account {uid}", flush=True)

    def measure_requests(self) -> None:
        start = now_utc()
        result = self.probe(
            count=self.args.requests, folder=f"perf-gate-{self.gate_id}"
        )
        time.sleep(1.2)
        lines = in_window(access_lines(start), start, now_utc())
        # The probe's own setup sends one session-folder create before the
        # timed calls; backend init and the folder delete are not MKCOLs.
        mkcols = [line for line in lines if line["method"] == "MKCOL"]
        searches = [line for line in lines if "/cloud/users/details" in line["path"]]
        n = max(1, self.args.requests)
        self.report["request"] = {
            "agent_service": {
                **summary_ms(result["agent"]),
                "requests_per_call": round((len(mkcols) - 1) / n, 2),
                # "app_password" once WP1b adopted one, else "password".
                "secret": result.get("agent_secret"),
            },
            "admin": {
                **summary_ms(result["admin"]),
                "requests_per_call": round(len(searches) / n, 2),
            },
            "by_identity": by_identity(lines),
        }

    def _create(self, label: str) -> tuple[str, float]:
        body = {
            "title": f"nextcloud perf gate {self.gate_id} {label}",
            "config_name": "session_base",
            # The outbox alone, as a user attaches it: a session gets its
            # Nextcloud folder only for an attached outbox, which a lite
            # (virtual) workspace does not offer.
            "datasource_ids": [self.outbox],
            "config_override": {"workspace": {"backend": "sandbox"}},
        }
        created, seconds = self.api("POST", "/api/persistent/threads", body)
        thread = created.get("id") or created.get("thread_id")
        if not thread:
            raise GateError(f"create returned no thread id: {sorted(created)}")
        self.created_threads.append(thread)
        return thread, seconds

    def _create_phase(self, name: str, count: int) -> dict[str, Any]:
        time.sleep(1.2)
        start = now_utc()
        threads, walls = [], []
        for index in range(count):
            thread, seconds = self._create(f"{name}-{index + 1}")
            threads.append(thread)
            walls.append(seconds)
        end = now_utc()
        time.sleep(1.2)
        lines = in_window(access_lines(start), start, end)
        folders = sql(
            "SELECT count(*) FROM threads WHERE main_cloud_session_handle IS NOT NULL "
            "AND id IN (" + ",".join(lit(t) for t in threads) + ")"
        )
        shares = sql(
            "SELECT count(*) FROM threads WHERE main_cloud_share_handle IS NOT NULL "
            "AND id IN (" + ",".join(lit(t) for t in threads) + ")"
        )
        logs = command(
            K
            + ["logs", ORCHESTRATOR, "-c", "orchestrator"]
            + [f"--since-time={since_arg(start)}"]
        )
        timings = [
            line.split("thread-create timing: ", 1)[1]
            for line in logs.splitlines()
            if "thread-create timing: " in line
            and any(f"thread={t}" in line for t in threads)
        ]
        return {
            "sessions": count,
            "with_folder": int(folders or 0),
            "shared": int(shares or 0),
            "wall": summary_ms(walls),
            "nextcloud_requests_per_create": round(len(lines) / max(1, count), 2),
            "by_identity": by_identity(lines),
            "thread_create_timing": timings,
        }

    def measure_creates(self) -> None:
        # Empty the user's Nextcloud identity cache: the first create misses.
        sql(
            "UPDATE users SET cloud_identity = COALESCE(cloud_identity, '{}'::jsonb) "
            f"- 'nextcloud' WHERE id = {lit(self.user_id)}"
        )
        self.report["create"] = {
            "miss": self._create_phase("miss", 1),
            "hit": self._create_phase("hit", self.args.sessions),
        }

    def cleanup(self) -> None:
        errors = []
        for thread in self.created_threads:
            # A sandbox session's delete answers 503 while its workspace is
            # still being torn down, and finishes in the background.
            code = command(
                K
                + ["exec", "-i", ORCHESTRATOR, "-c", "orchestrator", "--"]
                + ["curl", "-sS", "-o", "/dev/null", "-w", "%{http_code}"]
                + ["-X", "DELETE"]
                + [
                    f"http://localhost:8085/api/persistent/threads/{thread}"
                    "?force=true&permanent=true"
                ]
                + ["-H", f"Authorization: Bearer {self.token}"],
                data="",
            )
            if code not in {"200", "204", "404", "503"}:
                errors.append(f"thread {thread}: DELETE answered {code}")
        if self.created_threads:
            ids = ",".join(lit(t) for t in self.created_threads)
            deadline = time.monotonic() + 300
            left = "?"
            while time.monotonic() < deadline:
                left = sql(f"SELECT count(*) FROM threads WHERE id IN ({ids})")
                if left == "0":
                    break
                time.sleep(3)
            if left != "0":
                errors.append(f"{left} sessions still present after 300 s")
        if self.created_nc_user:
            try:
                occ("user:delete", self.created_nc_user)
            except GateError as exc:
                errors.append(f"nextcloud user {self.created_nc_user}: {exc}")
        if self.user_id and self.saved_identity is not None:
            try:
                sql(
                    f"UPDATE users SET cloud_identity = {lit(self.saved_identity)}::jsonb "
                    f"WHERE id = {lit(self.user_id)}"
                )
            except GateError as exc:
                errors.append(f"cloud identity: {exc}")
        if errors:
            for error in errors:
                print(f"FAIL cleanup {error}", flush=True)
            self.failures.append("cleanup")
        else:
            print(
                f"PASS cleanup ({len(self.created_threads)} sessions"
                + (f", account {self.created_nc_user}" if self.created_nc_user else "")
                + ", identity cache restored)",
                flush=True,
            )

    # -------------------------------------------------------------- report
    def check(self, label: str, ok: bool, detail: str) -> None:
        print(f"{'PASS' if ok else 'FAIL'} {label}: {detail}", flush=True)
        if not ok:
            self.failures.append(label)

    def expectations(self) -> None:
        args = self.args
        hit = self.report["create"]["hit"]
        self.check(
            "every session got its folder",
            hit["with_folder"] == hit["sessions"],
            f"{hit['with_folder']}/{hit['sessions']}",
        )
        self.check(
            "every session was shared",
            hit["shared"] == hit["sessions"],
            f"{hit['shared']}/{hit['sessions']}",
        )
        if args.expect_create_requests is not None:
            got = hit["nextcloud_requests_per_create"]
            self.check(
                "Nextcloud requests per create (cache hit)",
                got <= args.expect_create_requests,
                f"{got} (expected <= {args.expect_create_requests})",
            )
        if args.expect_agent_ms is not None:
            got = self.report["request"]["agent_service"]["p50_ms"]
            self.check(
                "agent-service request p50",
                got <= args.expect_agent_ms,
                f"{got} ms (expected <= {args.expect_agent_ms} ms)",
            )
        if args.expect_token_rows_per_request is not None:
            got = self.report["tokens"]["rows_per_request"]
            self.check(
                "oc_authtoken rows per request",
                got <= args.expect_token_rows_per_request,
                f"{got} (expected <= {args.expect_token_rows_per_request})",
            )

    def run(self) -> int:
        # Log in first: a login refreshes the user's display name from the
        # token's claims, and the disposable account must carry the name the
        # orchestrator will search for.
        self.login()
        self.preflight()
        try:
            self.ensure_nc_account()
            tokens_before = token_rows()
            run_start = now_utc()
            self.measure_requests()
            self.measure_creates()
            time.sleep(1.2)
            sent = len(in_window(access_lines(run_start), run_start, now_utc()))
            tokens_after = token_rows()
            self.report["tokens"] = {
                "before": tokens_before,
                "after": tokens_after,
                "orchestrator_requests": sent,
                "rows_per_request": round(
                    (tokens_after - tokens_before) / max(1, sent), 3
                ),
            }
        finally:
            self.cleanup()
        print(json.dumps(self.report, indent=2), flush=True)
        self.expectations()
        if self.args.json:
            with open(self.args.json, "w", encoding="utf-8") as handle:
                json.dump(self.report, handle, indent=2)
        return 1 if self.failures else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    parser.add_argument("--run", action="store_true")
    parser.add_argument("--confirm")
    parser.add_argument("--gate-id")
    parser.add_argument("--sessions", type=int, default=8)
    parser.add_argument("--requests", type=int, default=10)
    parser.add_argument("--expect-create-requests", type=float)
    parser.add_argument("--expect-agent-ms", type=float)
    parser.add_argument("--expect-token-rows-per-request", type=float)
    parser.add_argument("--json")
    args = parser.parse_args()
    if not args.run:
        print(__doc__)
        print("Dry run: nothing was changed. Add --run --confirm LOCAL-K3D-DISPOSABLE.")
        return 0
    if args.confirm != LOCAL_CONFIRMATION:
        print(
            f"refusing to run without --confirm {LOCAL_CONFIRMATION}", file=sys.stderr
        )
        return 2
    try:
        return Gate(args).run()
    except GateError as exc:
        print(f"FAIL {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
