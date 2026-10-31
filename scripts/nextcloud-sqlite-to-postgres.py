#!/usr/bin/env python3
"""Operator runbook: move a bundled Nextcloud from SQLite to Postgres.

Design: knowledge-base/knowledge/features/bundled_nextcloud_performance.md,
WP6 (with WP4, which gives the Nextcloud container an empty Postgres and its
``POSTGRES_HOST``/``POSTGRES_DB``/``POSTGRES_USER``/``POSTGRES_PASSWORD``
environment). The conversion is ``occ db:convert-type``; this script wraps it
in the checks the migration needs. One phase per run, in this order:

  precheck  read-only. ``occ status``; ``dbtype`` must be ``sqlite3``; the
            target from the container environment (password reported as
            present or absent only) must be reachable, UTF8, writable by its
            user and hold no ``oc_`` table; per-table row counts, max ids,
            shares, users, group folder tables, ``user_oidc`` providers; every
            column whose longest value exceeds its declared VARCHAR/CHAR
            length (SQLite keeps but does not enforce it, Postgres does) and
            every text column holding a NUL byte (Postgres refuses both).
            Writes precheck.json.
  close     ``occ maintenance:mode --on``, then waits --settle-seconds (at
            least 60: Apache and the protected FPM sidecar revalidate opcache
            every 60 s, so either may serve the old config.php until then),
            then until no cron.php runs in the cron container (maintenance
            stops new runs only; a running one keeps writing until it ends).
            Prints what the operator must confirm (no protected engage or
            Apply in flight, session creates paused) and an advisory count of
            protected mounts mid-engage from the SRW database. Writes close.json.
  backup    streams the SQLite database (and its -wal/-shm) and config.php
            out with ``tar`` over ``kubectl exec`` stdout (never ``kubectl
            cp``, which once truncated a database copy silently), compares md5
            in the pod (before and after) with the local copy, runs ``PRAGMA
            integrity_check`` in the pod and on the local copy, records the
            row inventory, the digests of instanceid/secret/passwordsalt and a
            few sample files (read by fileid) that ``verify`` compares against.
            Needs maintenance on and settled. ``--hot`` is a read-only
            rehearsal on a live instance: it writes to <backup-dir>/hot/ and is
            labelled "hot copy, not a migration backup".
  convert   needs maintenance on and settled, a verified backup and an
            unchanged database since. Runs, in one shell line in the
            container: ``printf '%s' "$POSTGRES_PASSWORD" | php occ
            db:convert-type -n --all-apps --port <port> pgsql <user> <host>
            <db>``, then ``maintenance:mode --on`` whatever it returned:
            convert-type turns maintenance OFF itself on success and on a
            failed copy. A failure leaves config.php on SQLite; a retry needs
            a fresh backup (convert-type itself writes the SQLite file, see
            verify) and ``--clear-schema``. After a successful copy, still in
            maintenance, raises every sequence: convert-type resyncs each to
            max(id)+1, but SQLite's AUTOINCREMENT never reuses an id, so
            sqlite_sequence remembers ids of deleted rows that Postgres would
            otherwise hand out again (share ids SRW still holds, group folder
            ids with orphan rows, fileids behind /f/ links).
  verify    read-only, while still in maintenance. ``dbtype`` is ``pgsql``;
            the SQLite file as convert-type left it differs from the backup
            only by convert-type's own writes (``--all-apps`` boots the
            disabled apps: the encryption app adds its key ids to appconfig,
            the user mount cache is emptied); every one of its tables is on
            Postgres with the same row count; the max ids match; every
            sequence's next value is above both its column's max and SQLite's
            high-water mark for the table; encryption is still off. Then
            (``--restart``: a rollout restart, which clears opcache) ``occ
            status``; instanceid/secret/passwordsalt unchanged (by digest);
            the sample files resolve to the same fileid and etag and read the
            same bytes (on an object store by ``urn:oid:<fileid>``).
            Writes verify.json.
  open      THE POINT OF NO RETURN (see below). Needs a passing verify (with
            ``--restart``, or 60 s after the conversion) and a
            second token. ``occ maintenance:mode --off``, then
            ``db:add-missing-indices``, ``-columns``, ``-primary-keys``; prints
            the smoke checklist, the perf gate command and the database backup
            reminder.
  rollback  only before ``open``: restores the backed-up config.php into the
            pod (``kubectl exec -i``, written beside and renamed), restarts
            and shows ``dbtype`` back on ``sqlite3``. Refuses when maintenance
            is off unless ``--i-know-maintenance-was-off``.
  fix-sequences  repair for an install converted (and maybe opened) without
            the raise: takes the high-water marks from inventory.json, the
            local backup copy and the old SQLite file in the pod, whichever
            exist, and raises each sequence under a write lock on its table.
            Only ever raises, so it is safe on a live instance and a second
            run is a no-op. Ids handed out before the repair may already be
            reused; it cannot undo that.

After ``open`` the first write lands on Postgres. Going back to SQLite then
would hand out the same fileids again (overwriting their objects on an object
store) and lose every token, share and folder SRW recorded meanwhile;
converting back to SQLite is not supported.

The database password is never read by this script: it stays in the
container environment and reaches convert-type on stdin. No command line this
script builds carries a credential, and none is printed.

Every phase is a dry run (the plan, nothing executed, not even reads) unless
given ``--run --confirm MIGRATE-NEXTCLOUD-<namespace>``; ``open`` also needs
``--confirm-open OPEN-NEXTCLOUD-<namespace>``. The backup directory holds
config.php (secrets): it is created 0700 and refused inside a git work tree.

  python scripts/nextcloud-sqlite-to-postgres.py precheck \\
      --context k3d-srw --namespace srw --backup-dir ~/nc-wp6       # plan
  python scripts/nextcloud-sqlite-to-postgres.py precheck \\
      --context k3d-srw --namespace srw --backup-dir ~/nc-wp6 \\
      --run --confirm MIGRATE-NEXTCLOUD-srw
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shlex
import shutil
import sqlite3
import subprocess
import sys
import tarfile
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

CONFIRM_PREFIX = "MIGRATE-NEXTCLOUD-"
OPEN_PREFIX = "OPEN-NEXTCLOUD-"
NC_ROOT = "/var/www/html"
#: ``opcache.revalidate_freq`` on Apache and the protected FPM sidecar.
SETTLE_MIN_SECONDS = 60
DEFAULT_SETTLE_SECONDS = 65
#: The Deployment uses ``progressDeadlineSeconds: 900``.
ROLLOUT_TIMEOUT = "900s"
EMPTY_MD5 = hashlib.md5(b"").hexdigest()
PHASES = (
    "precheck",
    "close",
    "backup",
    "convert",
    "verify",
    "open",
    "rollback",
    "fix-sequences",
)
_NAMESPACE_RE = re.compile(r"[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?\Z")
#: Local environment variables whose values must never reach a command line.
_SECRET_ENV_NAME = re.compile(r"PASSWORD|PASSWD|SECRET|TOKEN|_KEY\Z|CREDENTIAL", re.I)
_SECRET_MIN_LENGTH = 8
#: Declared SQLite types Postgres enforces a length on (Doctrine writes
#: ``VARCHAR(n)``; the others are accepted for completeness).
_LENGTH_TYPE_RE = re.compile(
    r"\s*(?:N?VARCHAR2?|CHARACTER\s+VARYING|VARYING\s+CHARACTER"
    r"|NATIONAL\s+(?:VARYING\s+)?CHARACTER|NATIVE\s+CHARACTER|N?CHAR|CHARACTER)"
    r"\s*\(\s*(\d+)\s*\)",
    re.I,
)
SAMPLE_MAX_BYTES = 8 * 1024 * 1024

POINT_OF_NO_RETURN = """\
============================================================================
POINT OF NO RETURN
Until now, rolling back is "restore the backed-up config.php" (the rollback
phase). Once maintenance mode goes off, Nextcloud writes to Postgres. From
the first write on, going back to SQLite is UNSAFE:
  - SQLite would hand out the same fileids again, and on an object store
    their urn:oid:<fileid> objects would be OVERWRITTEN;
  - every token, share, folder and grant SRW recorded meanwhile would be lost;
  - converting Postgres back to SQLite is not supported by Nextcloud.
Go now, or run the rollback phase instead.
============================================================================"""

SMOKE_CHECKLIST = """\
Smoke test (writes are allowed now), each must pass:
  1. a new stateless session is created and its session folder shared;
  2. a turn push lands, with ETags intact (no 412 storm in the agent log);
  3. a project-folder mount reads;
  4. the user-root connector verify passes (Reconnect not shown);
  5. the protected gate passes (protected mode on);
  6. a job export /f/<fileId> link opens.
Then run the perf gate (k3d) and compare with the pre-migration numbers:
  .venv/bin/python scripts/k3d-nextcloud-perf-gate.py --run --confirm LOCAL-K3D-DISPOSABLE"""

DB_BACKUP_REMINDER = """\
Database backup, now (printed only; this script does not run it):
  - CNPG engine (dev): kubectl cnpg backup srw-nextclouddb -n {namespace} --context {context}
    (or let the ScheduledBackup run with immediate: true; check the Backup CR
    reaches phase completed);
  - statefulset engine (k3d, university): pg_dump from the srw-nextclouddb pod
    to storage outside the cluster.
Keep the SQLite backup in {backup_dir} until the instance has run on Postgres
for a while; it is the only copy of the pre-migration state."""

OPEN_NOTES = """\
Also:
  - with Valkey on (WP5), truncate oc_file_locks once (it is unused from now on);
  - config.php now holds dbuser/dbpassword in plain text: rotating the database
    password also needs config.php's dbpassword changed (WP4);
  - background jobs (WP3) can be switched on now, so the first drain runs on
    Postgres."""

CLOSE_CHECKLIST = """\
Before running backup, the operator confirms:
  [ ] no protected engage or Apply is in flight (the advisory count above is
      only a hint; check the protected sessions you know of);
  [ ] session creates are paused or announced: cloud calls fail during the
      window (Nextcloud answers 503);
  [ ] the downtime is announced."""

PLANS: dict[str, list[str]] = {
    "precheck": [
        "[read] occ status; config.php (dbtype, data directory, digests)",
        "[read] container env POSTGRES_* (password: present or not); "
        "connect to the target with PHP PDO: version, UTF8, CREATE right, no oc_ tables",
        "[read] SQLite (read-only PDO): per-table rows, max ids, VARCHAR overruns, NUL bytes",
        "[read] SRW database (orchestrator pod): protected mounts mid-engage (advisory)",
        "[local] write precheck.json",
    ],
    "close": [
        "[read] config.php: dbtype must be sqlite3",
        "[read] SRW database (orchestrator pod): protected mounts mid-engage (advisory)",
        "[WRITE] occ maintenance:mode --on",
        "[wait] until maintenance has been on for --settle-seconds (>= 60)",
        "[read] occ status: maintenance on",
        "[wait] until no cron.php runs in the cron container (--cron-wait-seconds)",
        "[local] write close.json; print the operator checklist",
    ],
    "backup": [
        "[read] occ status (maintenance on and settled, no cron.php running; "
        "unless --hot)",
        "[read] md5 of nextcloud.db(-wal, -shm) and config.php in the pod",
        "[read] tar | gzip over kubectl exec stdout -> <backup-dir>/backup.tar.gz (hot: <backup-dir>/hot/)",
        "[read] md5 in the pod again; local md5 of the extracted files",
        "[read] PRAGMA integrity_check in the pod (read-only PDO) and locally (sqlite3)",
        "[read] inventory, config digests, sample files read by fileid",
        "[local] write backup.json and inventory.json",
    ],
    "convert": [
        "[read] dbtype sqlite3, maintenance on and settled, a verified backup, "
        "no cron.php running, database md5 unchanged since, target empty "
        "(or --clear-schema)",
        "[WRITE] in the container: printf '%s' \"$POSTGRES_PASSWORD\" | php occ "
        "db:convert-type -n --all-apps --port <port> pgsql <user> <host> <db>; "
        "php occ maintenance:mode --on",
        "[read] dbtype pgsql, maintenance on",
        "[WRITE] still in maintenance: raise every Postgres sequence to the higher "
        "of max(id) and SQLite's high-water mark (sqlite_sequence); never lowers one",
        "[local] write convert.log and convert.json",
    ],
    "verify": [
        "[read] dbtype pgsql",
        "[read] the SQLite file as convert-type left it vs inventory.json: only "
        "convert-type's own writes (appconfig rows added, mount cache emptied)",
        "[read] Postgres (read-only session) vs that SQLite: row counts, max ids, "
        "sequences past their max, encryption still as it was, user_oidc providers",
        "[WRITE, only with --restart] kubectl rollout restart + rollout status",
        "[read] dbtype pgsql; occ status; config digests vs the backup",
        "[read] sample files: same fileid/etag, same bytes",
        "[local] write verify.json",
    ],
    "open": [
        "[print] the point-of-no-return warning",
        "[read] verify.json passed for this backup (with --restart, or 60 s "
        "after the conversion); dbtype pgsql",
        "[WRITE] occ maintenance:mode --off",
        "[WRITE] occ db:add-missing-indices, db:add-missing-columns, db:add-missing-primary-keys",
        "[print] smoke checklist, perf gate command, database backup reminder",
    ],
    "rollback": [
        "[read] maintenance on (else refuse unless --i-know-maintenance-was-off); "
        "open.json absent; a verified backup",
        "[WRITE] write the backed-up config.php into the pod (beside, then rename)",
        "[WRITE] kubectl rollout restart + rollout status",
        "[read] dbtype sqlite3; occ status",
    ],
    "fix-sequences": [
        "[read] dbtype pgsql; SQLite high-water marks from inventory.json, the "
        "local backup copy and the old SQLite file in the pod (whichever exist)",
        "[WRITE] per table, locked against writes (reads go on): raise its "
        "sequence to the higher of max(id) and the high-water mark; never lowers "
        "one, so a second run changes nothing",
        "[local] write fix-sequences.json",
    ],
}


class MigrationError(RuntimeError):
    """A step failed or a precondition does not hold; nothing further runs."""


# --------------------------------------------------------------------------
# Pure helpers (unit-tested)
# --------------------------------------------------------------------------


def confirm_token(namespace: str) -> str:
    return CONFIRM_PREFIX + namespace


def open_token(namespace: str) -> str:
    return OPEN_PREFIX + namespace


def gate_error(args: argparse.Namespace) -> str | None:
    """Why this invocation may not run (``None``: it may, or it is a dry run)."""
    if not _NAMESPACE_RE.match(args.namespace or ""):
        return f"not a namespace name: {args.namespace!r}"
    if not args.run:
        return None
    if args.confirm != confirm_token(args.namespace):
        return f"refusing to run without --confirm {confirm_token(args.namespace)}"
    if args.phase == "open" and args.confirm_open != open_token(args.namespace):
        return (
            "open is the point of no return: it also needs "
            f"--confirm-open {open_token(args.namespace)}"
        )
    if args.phase == "close" and args.settle_seconds < SETTLE_MIN_SECONDS:
        return f"--settle-seconds must be at least {SETTLE_MIN_SECONDS} (opcache)"
    return None


def inside_git_worktree(path: Path) -> bool:
    """Whether ``path`` (existing or not) lies inside a git work tree."""
    current = path.expanduser().resolve()
    for candidate in (current, *current.parents):
        if (candidate / ".git").exists():
            return True
    return False


def secret_values(environ: dict[str, str]) -> list[str]:
    return [
        value
        for name, value in environ.items()
        if _SECRET_ENV_NAME.search(name) and len(value) >= _SECRET_MIN_LENGTH
    ]


def assert_argv_clean(argv: list[str], environ: dict[str, str]) -> None:
    """Refuse a command line that carries the value of a local secret variable."""
    for value in secret_values(environ):
        if any(value in part for part in argv):
            raise MigrationError(
                "refusing: a command line would carry the value of a secret "
                "environment variable"
            )


def declared_length(declared: str | None) -> int | None:
    """The length limit of a declared SQLite column type, or ``None``."""
    if not declared:
        return None
    match = _LENGTH_TYPE_RE.match(declared)
    return int(match.group(1)) if match else None


def varchar_overruns(inventory: dict[str, Any]) -> list[dict[str, Any]]:
    """Columns whose longest value exceeds the declared length."""
    found = []
    for table, info in sorted(inventory.get("tables", {}).items()):
        for column in info.get("columns", []):
            limit = declared_length(column.get("type"))
            longest = column.get("maxlen")
            if limit is not None and longest is not None and int(longest) > limit:
                found.append(
                    {
                        "table": table,
                        "column": column["name"],
                        "declared": column["type"],
                        "limit": limit,
                        "max_length": int(longest),
                    }
                )
    return found


def nul_findings(inventory: dict[str, Any]) -> list[dict[str, Any]]:
    """Text columns holding a NUL byte (Postgres text cannot)."""
    found = []
    for table, info in sorted(inventory.get("tables", {}).items()):
        for column in info.get("columns", []):
            if int(column.get("nul_rows") or 0) > 0:
                found.append(
                    {
                        "table": table,
                        "column": column["name"],
                        "rows": int(column["nul_rows"]),
                    }
                )
    return found


def table_counts(inventory: dict[str, Any]) -> dict[str, int]:
    return {
        table: int(info["rows"]) for table, info in inventory.get("tables", {}).items()
    }


def id_columns(inventory: dict[str, Any]) -> list[tuple[str, str]]:
    """Integer primary-key columns, whose max must survive the conversion."""
    return [
        (table, column["name"])
        for table, info in sorted(inventory.get("tables", {}).items())
        for column in info.get("columns", [])
        if "max" in column
    ]


def max_ids(inventory: dict[str, Any]) -> dict[str, int | None]:
    return {
        f"{table}.{column['name']}": _int_or_none(column.get("max"))
        for table, info in inventory.get("tables", {}).items()
        for column in info.get("columns", [])
        if "max" in column
    }


def next_sequence_value(last_value: int, is_called: bool, increment: int = 1) -> int:
    """The value ``nextval`` hands out next."""
    return last_value + increment if is_called else last_value


def effective_wal(md5: str | None) -> str | None:
    """A missing WAL and an empty one hold the same (no) data."""
    return None if md5 in (None, EMPTY_MD5) else md5


@dataclass
class Finding:
    level: str  # PASS / FAIL / WARN / INFO
    label: str
    detail: str


#: Tables a booting Nextcloud adds rows to even in maintenance mode: the
#: chart's before-starting hook re-applies app settings on every start, and
#: the user mount cache refills on any filesystem setup. On Postgres they may
#: hold MORE rows than the SQLite source after a restart; fewer is a loss.
GROWTH_TOLERATED = ("appconfig", "mounts")


def compare_inventories(
    sqlite_inventory: dict[str, Any],
    postgres: dict[str, Any],
    prefix: str = "oc_",
    high_water: dict[str, int] | None = None,
) -> list[Finding]:
    """What a converted Postgres must match of the SQLite it was copied from."""
    findings: list[Finding] = []
    before = table_counts(sqlite_inventory)
    after = {name.lower(): int(rows) for name, rows in postgres["tables"].items()}
    migrations = f"{prefix}migrations"
    growing = {f"{prefix}{name}" for name in GROWTH_TOLERATED}
    mismatched = 0
    for table, rows in sorted(before.items()):
        name = table.lower()
        if name not in after:
            findings.append(
                Finding("FAIL", f"table {table}", "missing on Postgres (not converted)")
            )
            mismatched += 1
        elif name == migrations:
            # convert-type fills it by running the migrations, --all-apps
            # included, so it may hold more rows than SQLite did.
            level = "INFO" if after[name] >= rows else "FAIL"
            findings.append(
                Finding(level, f"table {table}", f"{rows} -> {after[name]} rows")
            )
            mismatched += level == "FAIL"
        elif name in growing and after[name] > rows:
            findings.append(
                Finding(
                    "WARN",
                    f"table {table}",
                    f"{rows} -> {after[name]} rows: added since the conversion "
                    "(a restart re-applies app settings and refills the mount cache)",
                )
            )
        elif after[name] != rows:
            findings.append(
                Finding("FAIL", f"table {table}", f"{rows} rows -> {after[name]}")
            )
            mismatched += 1
    findings.append(
        Finding(
            "PASS" if not mismatched else "FAIL",
            "row counts",
            f"{len(before) - mismatched}/{len(before)} SQLite tables match on Postgres",
        )
    )
    extra = sorted(set(after) - {t.lower() for t in before})
    if extra:
        findings.append(
            Finding(
                "INFO",
                "tables only on Postgres",
                f"{len(extra)} (schemas of apps never enabled): {', '.join(extra[:12])}",
            )
        )
    expected_max = max_ids(sqlite_inventory)
    got_max = {key.lower(): _safe_int(v) for key, v in postgres["max"].items()}
    bad_max = []
    for key, value in sorted(expected_max.items()):
        got = got_max.get(key.lower())
        if got == value:
            continue
        grew = isinstance(got, int) and (value is None or got > value)
        if key.split(".", 1)[0].lower() in growing and grew:
            findings.append(
                Finding("WARN", f"max id {key}", f"{value} -> {got}: added since")
            )
            continue
        bad_max.append(f"{key} {value} -> {got}")
    findings.append(
        Finding(
            "FAIL" if bad_max else "PASS",
            "max ids",
            "; ".join(bad_max[:10])
            if bad_max
            else f"{len(expected_max)} columns equal",
        )
    )
    findings.extend(
        sequence_findings(postgres.get("sequences", []), high_water or {}, prefix)
    )
    return findings


def sequence_findings(
    sequences: list[dict[str, Any]], high_water: dict[str, int], prefix: str = "oc_"
) -> list[Finding]:
    """Every sequence must hand out ids above the table's max id AND above
    SQLite's AUTOINCREMENT high-water mark, which also counts deleted rows."""
    findings: list[Finding] = []
    behind = []
    covered = set()
    for sequence in sequences:
        table = str(sequence["table"]).lower()
        covered.add(table)
        if sequence.get("error"):
            behind.append(f"{sequence['sequence']}: {sequence['error']}")
            continue
        top = _int_or_none(sequence.get("max"))
        high = high_water.get(table)
        upcoming = next_sequence_value(
            int(sequence["last_value"]),
            bool(sequence["is_called"]),
            int(sequence.get("increment") or 1),
        )
        where = f"{sequence['table']}.{sequence['column']}"
        if top is not None and upcoming <= top:
            behind.append(f"{where} next {upcoming} <= max id {top}")
        elif high is not None and upcoming <= high:
            behind.append(
                f"{where} next {upcoming} <= SQLite high-water {high} "
                "(would reuse ids of deleted rows)"
            )
    findings.append(
        Finding(
            "FAIL" if behind else "PASS",
            "sequences past max id and SQLite high-water",
            "; ".join(behind[:10])
            if behind
            else f"{len(sequences)} sequences, {len(high_water)} high-water marks",
        )
    )
    unsequenced = sorted(
        t for t in high_water if t.startswith(prefix.lower()) and t not in covered
    )
    if unsequenced:
        findings.append(
            Finding(
                "WARN",
                "high-water marks without a sequence",
                ", ".join(unsequenced[:12])
                + ": no serial column on Postgres, so no sequence can reuse an id",
            )
        )
    return findings


def merge_high_water(*inventories: dict[str, Any] | None) -> dict[str, int]:
    """SQLite's AUTOINCREMENT marks (``sqlite_sequence``), highest per table.

    The mark is the largest id ever handed out, deleted rows included; it
    only grows, so the highest of several snapshots is the current one.
    """
    merged: dict[str, int] = {}
    for inventory in inventories:
        for table, seq in ((inventory or {}).get("sqlite_sequence") or {}).items():
            if seq is None:
                continue
            key = str(table).lower()
            merged[key] = max(merged.get(key, 0), int(seq))
    return merged


def marks_above_max(inventory: dict[str, Any]) -> list[tuple[str, int, int | None]]:
    """Tables whose SQLite high-water mark is above their largest id: the
    ones a plain max(id)+1 resync would hand out deleted rows' ids again."""
    tops: dict[str, int | None] = {}
    for key, value in max_ids(inventory).items():
        table = key.split(".", 1)[0].lower()
        tops[table] = value if tops.get(table) is None else max(tops[table], value or 0)
    above = []
    for table, mark in sorted(merge_high_water(inventory).items()):
        top = tops.get(table)
        if mark > (top or 0):
            above.append((table, mark, top))
    return above


def sequence_target(high: int | None, current_max: int | None) -> int:
    """The last value a sequence must have issued: above both marks."""
    return max(int(high or 0), int(current_max or 0))


def raise_findings(
    entries: list[dict[str, Any]], marks: dict[str, int], prefix: str = "oc_"
) -> list[Finding]:
    """One INFO per sequence the in-pod raise moved, a FAIL per one it could
    not, and a WARN for marks with no sequence on Postgres.

    A table SQLite tracked in sqlite_sequence but Postgres gives no serial
    column only warns: ids there are not drawn from a sequence, so none can
    be reused, and a table that is missing altogether already fails verify's
    row counts.
    """
    findings: list[Finding] = []
    failed = raised = 0
    covered = set()
    for e in entries:
        where = f"{e['table']}.{e['column']}"
        covered.add(str(e["table"]).lower())
        if e.get("error"):
            failed += 1
            findings.append(Finding("FAIL", f"sequence {where}", str(e["error"])))
            continue
        if int(e["next_after"]) <= int(e["target"]):
            failed += 1
            findings.append(
                Finding(
                    "FAIL",
                    f"sequence {where}",
                    f"next {e['next_after']} still <= {e['target']}",
                )
            )
            continue
        if e.get("raised"):
            raised += 1
            findings.append(
                Finding(
                    "INFO",
                    f"raised {where}",
                    f"next {e['next_before']} -> {e['next_after']} (SQLite high-water "
                    f"{e.get('high_water')}, max id {e.get('max')})",
                )
            )
    unsequenced = sorted(
        t for t in marks if t.startswith(prefix.lower()) and t not in covered
    )
    if unsequenced:
        findings.append(
            Finding(
                "WARN",
                "high-water marks without a sequence",
                ", ".join(unsequenced[:12])
                + ": no serial column on Postgres, so no sequence can reuse an id",
            )
        )
    findings.append(
        Finding(
            "FAIL" if failed else "PASS",
            "sequences above max id and SQLite high-water",
            f"{raised} raised, {len(entries) - raised - failed} already above, "
            f"{failed} failed; {len(marks)} high-water marks",
        )
    )
    return findings


def raised_next_value(
    last_value: int, is_called: bool, increment: int, target: int
) -> int:
    """What ``nextval`` hands out after the raise rule: ``setval(target)``
    only when the sequence would otherwise issue ``target`` or below."""
    upcoming = next_sequence_value(last_value, is_called, increment)
    return target + increment if upcoming <= target else upcoming


def source_drift(
    backup: dict[str, Any], source: dict[str, Any], prefix: str = "oc_"
) -> list[Finding]:
    """How the SQLite file changed between the backup and the end of convert.

    Maintenance mode keeps every client out, so the only writer left is
    convert-type itself. Rehearsed on 31.0.14, a first ``--all-apps`` run
    boots the disabled apps and so writes two things into its SOURCE before
    copying it: the encryption app records its key ids (rows added to
    appconfig; encryption stays off) and the user mount cache is emptied
    (Nextcloud rebuilds it on the next access). Anything else is unexpected.
    """
    before, after = table_counts(backup), table_counts(source)
    findings: list[Finding] = []
    unexpected = 0
    for table in sorted(set(before) | set(after)):
        old, new = before.get(table), after.get(table)
        if old == new:
            continue
        if table == f"{prefix}mounts" and old is not None and new is not None:
            findings.append(
                Finding(
                    "INFO",
                    f"source {table}",
                    f"{old} -> {new} rows: the user mount cache, emptied by "
                    "convert-type and rebuilt on the next access",
                )
            )
        elif (
            table == f"{prefix}appconfig"
            and old is not None
            and new is not None
            and new >= old
        ):
            findings.append(
                Finding(
                    "INFO",
                    f"source {table}",
                    f"{old} -> {new} rows: added by --all-apps booting the disabled "
                    "apps (the encryption app records its key ids)",
                )
            )
        else:
            unexpected += 1
            findings.append(
                Finding(
                    "FAIL",
                    f"source {table}",
                    f"{old} -> {new} rows after the backup: something besides "
                    "convert-type wrote the SQLite database",
                )
            )
    if backup.get("encryption_enabled") != source.get("encryption_enabled"):
        unexpected += 1
        findings.append(
            Finding(
                "FAIL",
                "source encryption_enabled",
                f"{backup.get('encryption_enabled')} -> {source.get('encryption_enabled')}",
            )
        )
    findings.append(
        Finding(
            "FAIL" if unexpected else "PASS",
            "SQLite source since the backup",
            f"{unexpected} unexpected changes"
            if unexpected
            else "changed only as convert-type changes it",
        )
    )
    return findings


def convert_shell(*, clear_schema: bool = False, chunk_size: int | None = None) -> str:
    """The one shell line (run as www-data) that converts and closes again.

    The password is read from the container environment by the shell and
    reaches convert-type on stdin, untrimmed (``printf``, never ``echo``):
    it is on no command line, and this script never sees it.
    """
    options = ["-n", "--all-apps"]
    if clear_schema:
        options.append("--clear-schema")
    if chunk_size:
        options.append(f"--chunk-size={int(chunk_size)}")
    convert = (
        "printf '%s' \"$POSTGRES_PASSWORD\" | php occ db:convert-type "
        + " ".join(options)
        + ' --port "$port" pgsql "$POSTGRES_USER" "$host" "$POSTGRES_DB"'
    )
    return "\n".join(
        [
            "set -u",
            f"cd {NC_ROOT}",
            'if [ -z "${POSTGRES_HOST:-}" ] || [ -z "${POSTGRES_DB:-}" ] \\',
            '   || [ -z "${POSTGRES_USER:-}" ] || [ -z "${POSTGRES_PASSWORD:-}" ]; then',
            '  echo "SRW-MIGRATE: the POSTGRES_* environment is incomplete" >&2',
            "  exit 90",
            "fi",
            'host="${POSTGRES_HOST%%:*}"',
            "port=5432",
            'case "$POSTGRES_HOST" in *:*) port="${POSTGRES_HOST##*:}" ;; esac',
            convert,
            "rc=$?",
            # convert-type sets maintenance OFF after a successful copy AND
            # when the copy throws: close again whatever it returned.
            "php occ maintenance:mode --on",
            "mrc=$?",
            'echo "SRW-MIGRATE: convert-type exit $rc; maintenance:mode --on exit $mrc"',
            'exit "$rc"',
        ]
    )


def _int_or_none(value: Any) -> int | None:
    if value is None or value == "":
        return None
    return int(value)


def _safe_int(value: Any) -> int | str | None:
    """An int, ``None``, or the text of an error the pod reported instead."""
    try:
        return _int_or_none(value)
    except (TypeError, ValueError):
        return str(value)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso(moment: datetime) -> str:
    return moment.strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_iso(text: str) -> datetime:
    return datetime.strptime(text, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


# --------------------------------------------------------------------------
# PHP run inside the Nextcloud container (as www-data, script on stdin)
# --------------------------------------------------------------------------

_PHP_PRELUDE = r"""<?php
ini_set('display_errors', 'stderr');
ini_set('zend.exception_ignore_args', '1');
error_reporting(E_ALL & ~E_DEPRECATED);
const NC_ROOT = '/var/www/html';

function nc_config_file(): array {
    $CONFIG = [];
    include NC_ROOT . '/config/config.php';
    return is_array($CONFIG) ? $CONFIG : [];
}

// As OC\Config reads it: config.php, then every *.config.php over it. A
// file that needs the booted server (apps.config.php names OC::$SERVERROOT)
// is skipped: none of them sets a database or storage key this script reads.
function nc_config(): array {
    $merged = nc_config_file();
    $extra = glob(NC_ROOT . '/config/*.config.php') ?: [];
    natsort($extra);
    foreach ($extra as $file) {
        $CONFIG = [];
        try {
            include $file;
        } catch (Throwable $e) {
            continue;
        }
        if (is_array($CONFIG)) {
            $merged = array_merge($merged, $CONFIG);
        }
    }
    return $merged;
}

function known_secrets(): array {
    $c = @nc_config_file();
    $env = getenv('POSTGRES_PASSWORD');
    return array_values(array_filter([
        $env === false ? null : $env,
        $c['dbpassword'] ?? null,
        $c['secret'] ?? null,
        $c['passwordsalt'] ?? null,
    ], fn ($v) => is_string($v) && $v !== ''));
}

function scrub(string $message): string {
    foreach (known_secrets() as $secret) {
        $message = str_replace($secret, '***', $message);
    }
    return $message;
}

function emit(array $out): void {
    echo 'RESULT ', json_encode($out, JSON_UNESCAPED_SLASHES | JSON_INVALID_UTF8_SUBSTITUTE), "\n";
}

set_exception_handler(function (Throwable $e) {
    emit(['error' => scrub(get_class($e) . ': ' . $e->getMessage())]);
    exit(3);
});

function prefix(array $cfg): string {
    return $cfg['dbtableprefix'] ?? 'oc_';
}

function like_prefix(string $prefix): string {
    return str_replace(['\\', '_', '%'], ['\\\\', '\\_', '\\%'], $prefix) . '%';
}

function qi(string $name): string {
    return '"' . str_replace('"', '""', $name) . '"';
}

function split_host(string $host, int $default = 5432): array {
    if (preg_match('/^(.+):(\d+)$/', $host, $m)) {
        return [$m[1], (int) $m[2]];
    }
    return [$host, $default];
}

// An SQLite file, read-only (it may no longer be the configured database).
function open_sqlite(string $path): array {
    if (!is_file($path)) {
        throw new RuntimeException('no SQLite database at ' . $path);
    }
    $pdo = new PDO('sqlite:' . $path, null, null, [
        PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
        PDO::SQLITE_ATTR_OPEN_FLAGS => PDO::SQLITE_OPEN_READONLY,
    ]);
    $pdo->exec('PRAGMA query_only = 1');
    $pdo->exec('PRAGMA busy_timeout = 10000');
    return [$pdo, 'sqlite', $path];
}

// The configured database, read-only (SQLite: read-only open; Postgres: a
// read-only session). Credentials come from config.php, inside the pod.
function open_db(array $cfg): array {
    $opts = [
        PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
    ];
    $type = $cfg['dbtype'] ?? 'sqlite3';
    if ($type === 'sqlite3' || $type === 'sqlite') {
        return open_sqlite(rtrim($cfg['datadirectory'] ?? NC_ROOT . '/data', '/') . '/'
            . ($cfg['dbname'] ?? 'owncloud') . '.db');
    }
    if ($type === 'pgsql') {
        return [open_pgsql($cfg, true), 'pgsql', null];
    }
    throw new RuntimeException('unsupported dbtype ' . $type);
}

// The Postgres config.php names (what Nextcloud itself connects with).
function open_pgsql(array $cfg, bool $readonly): PDO {
    [$host, $port] = split_host((string) ($cfg['dbhost'] ?? ''));
    $dsn = 'pgsql:host=' . $host . ';port=' . $port . ';dbname='
        . ($cfg['dbname'] ?? '') . ';connect_timeout=10';
    $pdo = new PDO($dsn, $cfg['dbuser'] ?? '', $cfg['dbpassword'] ?? '', [
        PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
    ]);
    if ($readonly) {
        $pdo->exec('SET default_transaction_read_only = on');
    }
    return $pdo;
}

// SQLite's AUTOINCREMENT marks: the largest id each table ever handed out,
// deleted rows included (max(id) forgets those).
function sqlite_high_water(PDO $pdo): array {
    $has = $pdo->query("SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = 'sqlite_sequence'")->fetchColumn();
    $marks = [];
    if ((int) $has > 0) {
        foreach ($pdo->query('SELECT name, seq FROM sqlite_sequence') as $r) {
            $marks[$r['name']] = max($marks[$r['name']] ?? 0, (int) $r['seq']);
        }
    }
    return $marks;
}

// Every serial (nextval default) or identity column of the prefixed tables,
// with the sequence behind it.
function serial_columns(PDO $pdo, string $p): array {
    $st = $pdo->prepare(
        "SELECT table_name, column_name, column_default,"
        . " pg_get_serial_sequence(format('%I.%I', table_schema, table_name), column_name) AS seq"
        . " FROM information_schema.columns WHERE table_schema = current_schema()"
        . " AND table_name LIKE :like ESCAPE '\\'"
        . " AND (column_default LIKE 'nextval(%' OR is_identity = 'YES') ORDER BY table_name, column_name"
    );
    $st->execute(['like' => like_prefix($p)]);
    $out = [];
    foreach ($st->fetchAll() as $r) {
        $seq = $r['seq'];
        if ($seq === null && preg_match("/nextval\\('([^']+)'/", (string) $r['column_default'], $m)) {
            $seq = $m[1];
        }
        $out[] = ['table' => $r['table_name'], 'column' => $r['column_name'], 'sequence' => $seq];
    }
    return $out;
}

function sequence_state(PDO $pdo, string $seq): array {
    $state = $pdo->query('SELECT last_value, is_called FROM ' . $seq)->fetch();
    $inc = $pdo->prepare('SELECT seqincrement FROM pg_sequence WHERE seqrelid = CAST(:s AS regclass)');
    $inc->execute(['s' => $seq]);
    return [(int) $state['last_value'], (bool) $state['is_called'], (int) $inc->fetchColumn() ?: 1];
}

// --all-apps boots the disabled encryption app during a conversion, which
// records its key ids; server-side encryption itself must stay as it was.
function encryption_enabled(PDO $pdo, string $p): ?string {
    $st = $pdo->prepare('SELECT configvalue FROM ' . qi($p . 'appconfig')
        . " WHERE appid = 'core' AND configkey = 'encryption_enabled'");
    $st->execute();
    $v = $st->fetchColumn();
    return $v === false ? null : (string) $v;
}

$INPUT = json_decode(<<<'SRWJSON'
__INPUT__
SRWJSON, true);
"""

_PHP_CONFIG_FACTS = r"""
$file = nc_config_file();
$cfg = nc_config();
$digest = fn ($key) => isset($file[$key]) ? hash('sha256', (string) $file[$key]) : null;
emit([
    'dbtype' => $cfg['dbtype'] ?? null,
    'dbname' => $cfg['dbname'] ?? null,
    'dbhost' => $cfg['dbhost'] ?? null,
    'dbuser' => $cfg['dbuser'] ?? null,
    'has_dbpassword' => isset($cfg['dbpassword']) && $cfg['dbpassword'] !== '',
    'datadirectory' => $cfg['datadirectory'] ?? NC_ROOT . '/data',
    'prefix' => prefix($cfg),
    'maintenance' => (bool) ($file['maintenance'] ?? false),
    'version' => $cfg['version'] ?? null,
    'objectstore' => !empty($cfg['objectstore']),
    'objectstore_multibucket' => !empty($cfg['objectstore_multibucket']),
    'digests' => [
        'instanceid' => $digest('instanceid'),
        'secret' => $digest('secret'),
        'passwordsalt' => $digest('passwordsalt'),
    ],
]);
"""

_PHP_TARGET = r"""
$env = function (string $k): string { $v = getenv($k); return $v === false ? '' : $v; };
$cfg = nc_config();
$host = $env('POSTGRES_HOST');
$db = $env('POSTGRES_DB');
$user = $env('POSTGRES_USER');
$out = [
    'host' => $host, 'db' => $db, 'user' => $user,
    'password_set' => $env('POSTGRES_PASSWORD') !== '',
];
if ($host === '' || $db === '' || $user === '' || !$out['password_set']) {
    emit($out);
    exit(0);
}
[$h, $port] = split_host($host);
$out['port'] = $port;
try {
    $pdo = new PDO(
        'pgsql:host=' . $h . ';port=' . $port . ';dbname=' . $db . ';connect_timeout=10',
        $user, $env('POSTGRES_PASSWORD'),
        [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION, PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC]
    );
    $pdo->exec('SET default_transaction_read_only = on');
    $out['reachable'] = true;
    $out['server_version'] = $pdo->query('SHOW server_version')->fetchColumn();
    $out['encoding'] = $pdo->query('SHOW server_encoding')->fetchColumn();
    $out['current_user'] = $pdo->query('SELECT current_user')->fetchColumn();
    $schema = $pdo->query('SELECT current_schema()')->fetchColumn();
    $out['schema'] = $schema;
    if ($schema !== null && $schema !== false) {
        $st = $pdo->prepare("SELECT has_schema_privilege(current_schema(), 'CREATE')");
        $st->execute();
        $out['can_create'] = (bool) $st->fetchColumn();
        $st = $pdo->prepare(
            "SELECT count(*) FILTER (WHERE table_name LIKE :like ESCAPE '\\') AS prefixed,"
            . " count(*) AS total FROM information_schema.tables"
            . " WHERE table_schema = current_schema()"
        );
        $st->execute(['like' => like_prefix(prefix($cfg))]);
        $row = $st->fetch();
        $out['prefixed_tables'] = (int) $row['prefixed'];
        $out['other_tables'] = (int) $row['total'] - (int) $row['prefixed'];
    }
} catch (Throwable $e) {
    $out['reachable'] = false;
    $out['error'] = scrub(get_class($e) . ': ' . $e->getMessage());
}
emit($out);
"""

_PHP_SQLITE_INVENTORY = r"""
$cfg = nc_config();
// After a conversion the SQLite file is no longer the configured database:
// verify reads it by path, as convert-type left it.
[$pdo, $driver, $path] = empty($INPUT['path']) ? open_db($cfg) : open_sqlite($INPUT['path']);
if ($driver !== 'sqlite') { throw new RuntimeException('the configured database is not SQLite'); }
$p = prefix($cfg);
$st = $pdo->prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE :like ESCAPE '\\' ORDER BY name");
$st->execute(['like' => like_prefix($p)]);
$names = $st->fetchAll(PDO::FETCH_COLUMN);
$tables = [];
foreach ($names as $table) {
    $columns = [];
    $select = ['count(*) AS n'];
    foreach ($pdo->query('PRAGMA table_info(' . qi($table) . ')') as $i => $c) {
        $type = strtoupper((string) $c['type']);
        $col = ['name' => $c['name'], 'type' => $c['type'], 'pk' => (int) $c['pk']];
        $texty = $type === '' || str_contains($type, 'CHAR') || str_contains($type, 'CLOB') || str_contains($type, 'TEXT');
        if ($texty) {
            $select[] = 'max(length(' . qi($c['name']) . ")) AS l$i";
            $select[] = 'sum(instr(CAST(' . qi($c['name']) . " AS BLOB), x'00') > 0) AS z$i";
        }
        if ((int) $c['pk'] > 0 && str_contains($type, 'INT')) {
            $select[] = 'max(' . qi($c['name']) . ") AS m$i";
        }
        $columns[$i] = $col;
    }
    $row = $pdo->query('SELECT ' . implode(', ', $select) . ' FROM ' . qi($table))->fetch();
    foreach ($columns as $i => &$col) {
        if (array_key_exists("l$i", $row)) {
            $col['maxlen'] = $row["l$i"] === null ? null : (int) $row["l$i"];
            $col['nul_rows'] = (int) $row["z$i"];
        }
        if (array_key_exists("m$i", $row)) {
            $col['max'] = $row["m$i"] === null ? null : (int) $row["m$i"];
        }
    }
    unset($col);
    $tables[$table] = ['rows' => (int) $row['n'], 'columns' => array_values($columns)];
}
$providers = [];
if (in_array($p . 'user_oidc_providers', $names, true)) {
    $providers = $pdo->query('SELECT id, identifier FROM ' . qi($p . 'user_oidc_providers') . ' ORDER BY id')->fetchAll();
}
$size = fn ($f) => is_file($f) ? filesize($f) : null;
emit([
    'driver' => 'sqlite',
    'prefix' => $p,
    'encryption_enabled' => encryption_enabled($pdo, $p),
    'db' => [
        'path' => $path,
        'size' => $size($path),
        'wal_size' => $size($path . '-wal'),
        'journal_mode' => $pdo->query('PRAGMA journal_mode')->fetchColumn(),
        'sqlite_version' => $pdo->query('SELECT sqlite_version()')->fetchColumn(),
        'page_size' => (int) $pdo->query('PRAGMA page_size')->fetchColumn(),
        'page_count' => (int) $pdo->query('PRAGMA page_count')->fetchColumn(),
        'freelist_count' => (int) $pdo->query('PRAGMA freelist_count')->fetchColumn(),
    ],
    'tables' => $tables,
    'sqlite_sequence' => sqlite_high_water($pdo),
    'oidc_providers' => array_map(fn ($r) => ['id' => (int) $r['id'], 'identifier' => $r['identifier']], $providers),
]);
"""

_PHP_SQLITE_SEQUENCE = r"""
[$pdo] = open_sqlite($INPUT['path']);
emit(['path' => $INPUT['path'], 'sqlite_sequence' => sqlite_high_water($pdo)]);
"""

# Raises every sequence to the higher of the table's max id and SQLite's
# high-water mark, never lowers one. Each table is locked against writes
# (EXCLUSIVE: reads go on) while its sequence is read and set, so a live
# instance cannot draw an id in between; lock_timeout bounds the wait.
# Connects with config.php's credentials: the role Nextcloud itself uses,
# which owns the sequences (POSTGRES_PASSWORD is install-time env only and
# may be absent or rotated later).
_PHP_RAISE_SEQUENCES = r"""
$cfg = nc_config();
if (($cfg['dbtype'] ?? '') !== 'pgsql') { throw new RuntimeException('the configured database is not Postgres'); }
$pdo = open_pgsql($cfg, false);
$pdo->exec("SET lock_timeout = '10s'");
$high = $INPUT['high_water'];
$out = [];
foreach (serial_columns($pdo, prefix($cfg)) as $c) {
    $entry = $c;
    try {
        $pdo->beginTransaction();
        $pdo->exec('LOCK TABLE ' . qi($c['table']) . ' IN EXCLUSIVE MODE');
        [$last, $called, $inc] = sequence_state($pdo, $c['sequence']);
        $max = $pdo->query('SELECT max(' . qi($c['column']) . ') FROM ' . qi($c['table']))->fetchColumn();
        $mark = $high[strtolower($c['table'])] ?? null;
        $next = $called ? $last + $inc : $last;
        $target = max((int) ($mark ?? 0), (int) ($max ?? 0));
        $entry += [
            'max' => $max === null ? null : (int) $max, 'high_water' => $mark,
            'target' => $target, 'next_before' => $next, 'increment' => $inc,
        ];
        if ($next <= $target) {
            $set = $pdo->prepare('SELECT setval(CAST(:s AS regclass), CAST(:t AS bigint), true)');
            $set->execute(['s' => $c['sequence'], 't' => $target]);
            $entry['raised'] = true;
        } else {
            $entry['raised'] = false;
        }
        [$last, $called, $inc] = sequence_state($pdo, $c['sequence']);
        $entry['next_after'] = $called ? $last + $inc : $last;
        $pdo->commit();
    } catch (Throwable $e) {
        if ($pdo->inTransaction()) { $pdo->rollBack(); }
        $entry['error'] = scrub($e->getMessage());
    }
    $out[] = $entry;
}
emit(['sequences' => $out]);
"""

_PHP_OVERRUN_ROWS = r"""
$cfg = nc_config();
[$pdo, $driver] = open_db($cfg);
$out = [];
foreach ($INPUT['columns'] as $c) {
    $where = ' FROM ' . qi($c['table']) . ' WHERE length(' . qi($c['column']) . ') > ' . (int) $c['limit'];
    $rows = (int) $pdo->query('SELECT count(*)' . $where)->fetchColumn();
    $ids = $pdo->query('SELECT rowid' . $where . ' ORDER BY rowid LIMIT 5')->fetchAll(PDO::FETCH_COLUMN);
    $out[$c['table'] . '.' . $c['column']] = ['rows' => $rows, 'rowids' => array_map('intval', $ids)];
}
emit(['columns' => $out]);
"""

_PHP_INTEGRITY = r"""
$cfg = nc_config();
[$pdo, $driver] = open_db($cfg);
if ($driver !== 'sqlite') { throw new RuntimeException('the configured database is not SQLite'); }
$rows = $pdo->query('PRAGMA integrity_check')->fetchAll(PDO::FETCH_COLUMN);
emit(['result' => array_slice($rows, 0, 20), 'count' => count($rows)]);
"""

_PHP_PG_INVENTORY = r"""
$cfg = nc_config();
[$pdo, $driver] = open_db($cfg);
if ($driver !== 'pgsql') { throw new RuntimeException('the configured database is not Postgres'); }
$p = prefix($cfg);
$st = $pdo->prepare(
    "SELECT table_name FROM information_schema.tables WHERE table_schema = current_schema()"
    . " AND table_type = 'BASE TABLE' AND table_name LIKE :like ESCAPE '\\' ORDER BY table_name"
);
$st->execute(['like' => like_prefix($p)]);
$names = $st->fetchAll(PDO::FETCH_COLUMN);
$tables = [];
foreach ($names as $table) {
    $tables[$table] = (int) $pdo->query('SELECT count(*) FROM ' . qi($table))->fetchColumn();
}
$lower = array_map('strtolower', $names);
$max = [];
foreach ($INPUT['idcols'] as [$table, $column]) {
    $i = array_search(strtolower($table), $lower, true);
    if ($i === false) { continue; }
    try {
        $v = $pdo->query('SELECT max(' . qi(strtolower($column)) . ') FROM ' . qi($names[$i]))->fetchColumn();
        $max["$table.$column"] = $v === null ? null : (int) $v;
    } catch (Throwable $e) {
        $max["$table.$column"] = 'error: ' . scrub($e->getMessage());
    }
}
$sequences = [];
foreach (serial_columns($pdo, $p) as $entry) {
    try {
        [$last, $called, $inc] = sequence_state($pdo, $entry['sequence']);
        $entry += ['last_value' => $last, 'is_called' => $called, 'increment' => $inc];
        $v = $pdo->query('SELECT max(' . qi($entry['column']) . ') FROM ' . qi($entry['table']))->fetchColumn();
        $entry['max'] = $v === null ? null : (int) $v;
    } catch (Throwable $e) {
        $entry['error'] = scrub($e->getMessage());
    }
    $sequences[] = $entry;
}
$providers = [];
if (in_array($p . 'user_oidc_providers', $names, true)) {
    $providers = $pdo->query('SELECT id, identifier FROM ' . qi($p . 'user_oidc_providers') . ' ORDER BY id')->fetchAll();
}
emit([
    'driver' => 'pgsql',
    'encryption_enabled' => encryption_enabled($pdo, $p),
    'server_version' => $pdo->query('SHOW server_version')->fetchColumn(),
    'tables' => $tables,
    'max' => $max,
    'sequences' => $sequences,
    'oidc_providers' => array_map(fn ($r) => ['id' => (int) $r['id'], 'identifier' => $r['identifier']], $providers),
]);
"""

_PHP_SAMPLES = r"""
$cfg = nc_config();
[$pdo, $driver] = open_db($cfg);
$p = prefix($cfg);

function read_md5(array $cfg, string $storage, string $path, int $fileid): array {
    if (!empty($cfg['objectstore_multibucket'])) {
        return ['error' => 'a multibucket object store is not supported: read a file by hand'];
    }
    if (!empty($cfg['objectstore'])) {
        require_once NC_ROOT . '/lib/base.php';
        $poc = \OCP\Server::get(\OC\Files\ObjectStore\PrimaryObjectStoreConfig::class);
        if ($poc->hasMultipleObjectStorages()) {
            return ['error' => 'several object stores are configured: read a file by hand'];
        }
        $conf = $poc->getObjectStoreConfigForRoot();
        $store = $poc->buildObjectStore($conf);
        $urnPrefix = $conf['arguments']['objectPrefix'] ?? 'urn:oid:';
        $stream = $store->readObject($urnPrefix . $fileid);
        $ctx = hash_init('md5');
        hash_update_stream($ctx, $stream);
        fclose($stream);
        return ['md5' => hash_final($ctx), 'via' => 'object ' . $urnPrefix . $fileid];
    }
    if (str_starts_with($storage, 'home::')) {
        $file = rtrim($cfg['datadirectory'] ?? NC_ROOT . '/data', '/') . '/' . substr($storage, 6) . '/' . $path;
    } elseif (str_starts_with($storage, 'local::')) {
        $file = rtrim(substr($storage, 7), '/') . '/' . $path;
    } else {
        return ['error' => 'storage ' . $storage . ' is neither home:: nor local::'];
    }
    if (!is_file($file)) {
        return ['error' => 'not on disk'];
    }
    return ['md5' => md5_file($file), 'via' => 'filesystem path (no fileid in the object path)'];
}

$rows = [];
if ($INPUT['mode'] === 'pick') {
    $dir = $pdo->query("SELECT id FROM " . qi($p . 'mimetypes') . " WHERE mimetype = 'httpd/unix-directory'")->fetchColumn();
    $from = ' FROM ' . qi($p . 'filecache') . ' f JOIN ' . qi($p . 'storages') . ' s ON s.numeric_id = f.storage'
        . ' WHERE f.mimetype <> ' . (int) $dir . ' AND f.size > 0 AND f.size <= ' . (int) $INPUT['max_bytes']
        . " AND (f.path LIKE 'files/%' OR f.path LIKE '\\_\\_groupfolders/%' ESCAPE '\\')"
        . " AND (s.id LIKE 'home::%' OR s.id LIKE 'local::%' OR s.id LIKE 'object::%')";
    $n = (int) $pdo->query('SELECT count(*)' . $from)->fetchColumn();
    $offsets = array_values(array_unique(array_filter([0, intdiv($n, 2), $n - 1], fn ($o) => $o >= 0 && $o < $n)));
    foreach ($offsets as $o) {
        $rows[] = $pdo->query(
            'SELECT f.fileid, s.id AS storage, f.path, f.etag, f.size, f.mtime' . $from . ' ORDER BY f.fileid LIMIT 1 OFFSET ' . (int) $o
        )->fetch();
    }
} else {
    $st = $pdo->prepare(
        'SELECT f.fileid, s.id AS storage, f.path, f.etag, f.size, f.mtime FROM ' . qi($p . 'filecache') . ' f'
        . ' JOIN ' . qi($p . 'storages') . ' s ON s.numeric_id = f.storage WHERE s.id = :sid AND f.path_hash = :ph'
    );
    foreach ($INPUT['samples'] as $sample) {
        $st->execute(['sid' => $sample['storage'], 'ph' => md5($sample['path'])]);
        $row = $st->fetch();
        $rows[] = $row ?: ['missing' => true, 'storage' => $sample['storage'], 'path' => $sample['path']];
    }
}
$out = [];
foreach ($rows as $row) {
    if (!empty($row['missing'])) {
        $out[] = $row;
        continue;
    }
    $entry = [
        'fileid' => (int) $row['fileid'], 'storage' => $row['storage'], 'path' => $row['path'],
        'etag' => $row['etag'], 'size' => (int) $row['size'], 'mtime' => (int) $row['mtime'],
    ];
    try {
        $entry += read_md5($cfg, $row['storage'], $row['path'], (int) $row['fileid']);
    } catch (Throwable $e) {
        $entry['error'] = scrub(get_class($e) . ': ' . $e->getMessage());
    }
    $out[] = $entry;
}
emit(['samples' => $out]);
"""

_SRW_ADVISORY = r"""
import asyncio, json
import asyncpg
from shared.db_url import build_postgres_url


async def main():
    conn = await asyncpg.connect(build_postgres_url("POSTGRES", fallback_env="DATABASE_URL"), timeout=10)
    try:
        async with conn.transaction(readonly=True):
            mounts = await conn.fetch(
                "SELECT status, count(*) AS n FROM cloud_ro_mounts "
                "WHERE status IN ('engaging', 'active', 'revoking') GROUP BY status"
            )
            planned = await conn.fetchval(
                "SELECT count(*) FROM cloud_ro_effect_intents WHERE status = 'planned'"
            )
    finally:
        await conn.close()
    return {"mounts": {r["status"]: r["n"] for r in mounts}, "planned_effect_intents": planned}


print("RESULT " + json.dumps(asyncio.run(main())))
"""


def php_script(body: str, payload: Any = None) -> str:
    data = json.dumps(payload if payload is not None else {})
    if "\nSRWJSON" in data:
        raise MigrationError("refusing a PHP payload that would end its heredoc")
    return _PHP_PRELUDE.replace("__INPUT__", data) + body


# --------------------------------------------------------------------------
# Cluster access
# --------------------------------------------------------------------------

Runner = Callable[..., subprocess.CompletedProcess]


class Kube:
    """kubectl against one Nextcloud Deployment; every exec runs as www-data."""

    def __init__(
        self,
        context: str,
        namespace: str,
        deployment: str,
        container: str,
        *,
        runner: Runner | None = None,
        popen: Callable[..., Any] | None = None,
        environ: dict[str, str] | None = None,
    ) -> None:
        self.context = context
        self.namespace = namespace
        self.deployment = deployment
        self.container = container
        self.runner = runner or subprocess.run
        self.popen = popen or subprocess.Popen
        self.environ = dict(os.environ if environ is None else environ)

    def base(self) -> list[str]:
        return ["kubectl", f"--context={self.context}", "-n", self.namespace]

    def checked(self, argv: list[str]) -> list[str]:
        assert_argv_clean(argv, self.environ)
        return argv

    def exec_argv(
        self, shell: str, *, stdin: bool = False, container: str | None = None
    ) -> list[str]:
        """``shell`` as www-data in the container; it must name no secret."""
        return self.checked(
            self.base()
            + ["exec"]
            + (["-i"] if stdin else [])
            + [f"deploy/{self.deployment}", "-c", container or self.container, "--"]
            + ["su", "-s", "/bin/sh", "www-data", "-c", shell]
        )

    def occ_argv(self, *args: str) -> list[str]:
        return self.exec_argv(
            f"cd {NC_ROOT} && php occ " + " ".join(shlex.quote(a) for a in args)
        )

    def php_argv(self) -> list[str]:
        return self.exec_argv(f"cd {NC_ROOT} && php", stdin=True)

    def kubectl_argv(self, *args: str) -> list[str]:
        return self.checked(self.base() + list(args))

    def run(
        self,
        argv: list[str],
        *,
        data: str | bytes | None = None,
        timeout: float | None = 600,
        check: bool = True,
    ) -> subprocess.CompletedProcess:
        binary = isinstance(data, bytes)
        try:
            result = self.runner(
                argv,
                input=data,
                text=not binary,
                capture_output=True,
                timeout=timeout,
            )
        except subprocess.TimeoutExpired as exc:
            raise MigrationError(f"command timed out after {exc.timeout} s") from None
        if check and result.returncode:
            raise MigrationError(
                f"command failed (exit {result.returncode}): {_tail(result)}"
            )
        return result

    def occ(self, *args: str, check: bool = True) -> str:
        return _text(self.run(self.occ_argv(*args), check=check).stdout).strip()

    def sh(self, shell: str, *, check: bool = True) -> str:
        return _text(self.run(self.exec_argv(shell), check=check).stdout)

    def php(self, body: str, payload: Any = None, *, timeout: float = 1800) -> dict:
        result = self.run(
            self.php_argv(),
            data=php_script(body, payload),
            timeout=timeout,
            check=False,
        )
        for line in _text(result.stdout).splitlines():
            if line.startswith("RESULT "):
                parsed = json.loads(line[len("RESULT ") :])
                if "error" in parsed and len(parsed) == 1:
                    raise MigrationError(f"in-pod PHP: {parsed['error']}")
                return parsed
        raise MigrationError(
            f"in-pod PHP printed no result (exit {result.returncode}): {_tail(result)}"
        )

    def kubectl(self, *args: str, timeout: float | None = 600) -> str:
        return _text(self.run(self.kubectl_argv(*args), timeout=timeout).stdout)

    def stream_to_file(self, argv: list[str], path: Path) -> tuple[int, str]:
        """Run ``argv`` with stdout written to ``path`` (0600); (exit, stderr)."""
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "wb") as handle:
            process = self.popen(argv, stdout=handle, stderr=subprocess.PIPE)
            _, err = process.communicate()
        return process.returncode, _text(err)


def _text(value: str | bytes | None) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    return value


def _tail(result: subprocess.CompletedProcess) -> list[str]:
    # Command lines are never printed: they may name a credential's location.
    lines = _text(result.stderr).strip().splitlines()[-4:]
    lines += _text(result.stdout).strip().splitlines()[-4:]
    return lines


# --------------------------------------------------------------------------
# Phases
# --------------------------------------------------------------------------


@dataclass
class Report:
    failures: list[str] = field(default_factory=list)

    def line(self, level: str, label: str, detail: str = "") -> None:
        print(f"{level} {label}" + (f": {detail}" if detail else ""), flush=True)
        if level == "FAIL":
            self.failures.append(label)

    def check(self, ok: bool, label: str, detail: str = "") -> bool:
        self.line("PASS" if ok else "FAIL", label, detail)
        return ok

    def finding(self, item: Finding) -> None:
        self.line(item.level, item.label, item.detail)


def write_json(path: Path, data: Any) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        json.dump(data, handle, indent=2, sort_keys=True)
        handle.write("\n")


def read_json(path: Path) -> dict[str, Any] | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None


def local_high_water(files: Path) -> dict[str, Any] | None:
    """sqlite_sequence of the local backup copy (``None``: no copy there).

    Read from a scratch copy: opening the database with its WAL beside it
    would fold the WAL in, changing the backed-up file.
    """
    databases = sorted(files.glob("*.db")) if files.is_dir() else []
    if not databases:
        return None
    scratch = files.parent / "high-water-read"
    shutil.rmtree(scratch, ignore_errors=True)
    scratch.mkdir(mode=0o700)
    try:
        for name in (databases[0].name, databases[0].name + "-wal"):
            if (files / name).is_file():
                shutil.copy2(files / name, scratch / name)
        conn = sqlite3.connect(scratch / databases[0].name)
        try:
            exists = conn.execute(
                "SELECT count(*) FROM sqlite_master "
                "WHERE type = 'table' AND name = 'sqlite_sequence'"
            ).fetchone()[0]
            rows = (
                conn.execute("SELECT name, seq FROM sqlite_sequence").fetchall()
                if exists
                else []
            )
        finally:
            conn.close()
    finally:
        shutil.rmtree(scratch, ignore_errors=True)
    marks: dict[str, int] = {}
    for name, seq in rows:
        marks[name] = max(marks.get(name, 0), int(seq or 0))
    return {"sqlite_sequence": marks}


def md5_file(path: Path) -> str:
    digest = hashlib.md5()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


class Migration:
    def __init__(
        self,
        args: argparse.Namespace,
        kube: Kube,
        *,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.args = args
        self.kube = kube
        self.report = Report()
        self.dir = Path(args.backup_dir).expanduser().resolve()
        self.sleep = sleep

    # ----------------------------------------------------------- shared reads
    def status(self) -> dict[str, Any]:
        out = self.kube.occ("status", "--output=json")
        for line in out.splitlines():
            if line.startswith("{"):
                return json.loads(line)
        raise MigrationError("occ status printed no JSON")

    def facts(self) -> dict[str, Any]:
        return self.kube.php(_PHP_CONFIG_FACTS)

    def target(self) -> dict[str, Any]:
        return self.kube.php(_PHP_TARGET)

    def db_files(self, datadir: str, dbname: str) -> dict[str, int]:
        names = [f"{dbname}.db", f"{dbname}.db-wal", f"{dbname}.db-shm"]
        out = self.kube.sh(
            f"cd {shlex.quote(datadir)} && for f in "
            + " ".join(shlex.quote(n) for n in names)
            + '; do [ -f "$f" ] && stat -c \'%s %n\' -- "$f"; done; true'
        )
        sizes = {}
        for line in out.splitlines():
            size, _, name = line.strip().partition(" ")
            if name:
                sizes[name] = int(size)
        return sizes

    def pod_md5(self, datadir: str, names: list[str]) -> dict[str, str]:
        out = self.kube.sh(
            f"cd {shlex.quote(datadir)} && md5sum -- "
            + " ".join(shlex.quote(n) for n in names)
            + f" && cd {NC_ROOT}/config && md5sum -- config.php"
        )
        sums = {}
        for line in out.splitlines():
            digest, _, name = line.strip().partition("  ")
            if name:
                sums[name] = digest
        return sums

    def cron_runs(self) -> int | None:
        """Running cron.php in the cron container (``None``: there is none).

        Maintenance mode only stops NEW runs: one already running keeps
        writing the database until it ends.
        """
        names = self.kube.kubectl(
            "get",
            f"deployment/{self.kube.deployment}",
            "-o",
            "jsonpath={.spec.template.spec.containers[*].name}",
        ).split()
        if self.args.cron_container not in names:
            return None
        result = self.kube.run(
            self.kube.exec_argv(
                "pgrep -f '/var/www/html/cron[.]php' | wc -l",
                container=self.args.cron_container,
            ),
            check=False,
        )
        return int(_text(result.stdout).strip() or 0)

    def no_cron_running(self, *, wait: float = 0) -> None:
        deadline = time.monotonic() + wait
        while True:
            runs = self.cron_runs()
            if runs is None:
                self.report.line(
                    "INFO", "cron container", f"no {self.args.cron_container!r} here"
                )
                return
            if runs == 0:
                self.report.line("PASS", "no cron.php running")
                return
            if time.monotonic() >= deadline:
                raise MigrationError(
                    f"{runs} cron.php still running: it writes the database until "
                    "it ends; wait for it and run this phase again"
                )
            self.sleep(5)

    def maintenance_settled(self) -> None:
        close = read_json(self.dir / "close.json")
        if not close:
            raise MigrationError(
                "no close.json: run the close phase first (it is idempotent and "
                "waits for opcache)"
            )
        since = parse_iso(close["maintenance_on_at"])
        waited = (utc_now() - since).total_seconds()
        if waited < SETTLE_MIN_SECONDS:
            raise MigrationError(
                f"maintenance has been on for {int(waited)} s only; opcache may still "
                f"serve the old config.php for up to {SETTLE_MIN_SECONDS} s"
            )

    def advisory(self) -> None:
        try:
            argv = self.kube.checked(
                self.kube.base()
                + ["exec", "-i", f"deploy/{self.args.orchestrator_deployment}"]
                + ["-c", self.args.orchestrator_container, "--", "python", "-"]
            )
            result = self.kube.run(argv, data=_SRW_ADVISORY, timeout=60, check=False)
            line = next(
                (
                    raw
                    for raw in _text(result.stdout).splitlines()
                    if raw.startswith("RESULT ")
                ),
                None,
            )
            if not line:
                raise MigrationError(f"no result: {_tail(result)}")
            data = json.loads(line[len("RESULT ") :])
        except (MigrationError, json.JSONDecodeError) as exc:
            self.report.line("WARN", "advisory SRW check unavailable", str(exc)[:300])
            return
        mounts = data["mounts"]
        busy = int(mounts.get("engaging", 0)) + int(mounts.get("revoking", 0))
        planned = int(data["planned_effect_intents"] or 0)
        self.report.line(
            "WARN" if busy or planned else "INFO",
            "advisory: protected lane in the SRW database",
            f"{busy} mounts engaging/revoking, {mounts.get('active', 0)} active, "
            f"{planned} effect intents planned (not closed)",
        )

    # --------------------------------------------------------------- precheck
    def precheck(self) -> None:
        r = self.report
        status = self.status()
        r.check(
            bool(status.get("installed")), "installed", status.get("versionstring", "")
        )
        r.check(not status.get("needsDbUpgrade"), "no pending upgrade")
        r.line("INFO", "maintenance", "on" if status.get("maintenance") else "off")
        major = int(str(status.get("version", "0")).split(".")[0] or 0)
        if major >= 35:
            r.line(
                "WARN",
                "version",
                f"{major}: SQLite -> Postgres has open bugs on 35 (server #65389); "
                "convert on 31 before the major upgrades",
            )
        facts = self.facts()
        r.check(facts["dbtype"] == "sqlite3", "dbtype is sqlite3", str(facts["dbtype"]))
        if facts["objectstore_multibucket"]:
            r.line("WARN", "object store", "multibucket: sample reads are skipped")
        target = self.target()
        self.report_target(target, clear_schema=False)
        inventory = self.kube.php(_PHP_SQLITE_INVENTORY)
        self.report_inventory(inventory)
        overruns = varchar_overruns(inventory)
        detail: dict[str, Any] = {}
        if overruns:
            detail = self.kube.php(
                _PHP_OVERRUN_ROWS,
                {
                    "columns": [
                        {
                            "table": o["table"],
                            "column": o["column"],
                            "limit": o["limit"],
                        }
                        for o in overruns
                    ]
                },
            )["columns"]
        for o in overruns:
            rows = detail.get(f"{o['table']}.{o['column']}", {})
            r.line(
                "FAIL",
                f"VARCHAR overrun {o['table']}.{o['column']}",
                f"declared {o['declared']}, longest {o['max_length']}, "
                f"{rows.get('rows', '?')} rows (rowids {rows.get('rowids', [])}); "
                "Postgres refuses these and the copy stops",
            )
        if not overruns:
            r.line(
                "PASS", "VARCHAR lengths", "no value longer than its declared length"
            )
        nuls = nul_findings(inventory)
        for n in nuls:
            r.line(
                "FAIL",
                f"NUL byte {n['table']}.{n['column']}",
                f"{n['rows']} rows; Postgres text cannot hold NUL",
            )
        if not nuls:
            r.line("PASS", "NUL bytes", "none in text columns")
        self.advisory()
        write_json(
            self.dir / "precheck.json",
            {
                "at": iso(utc_now()),
                "status": status,
                "facts": {k: v for k, v in facts.items() if k != "digests"},
                "target": target,
                "inventory": inventory,
                "varchar_overruns": overruns,
                "overrun_rows": detail,
                "nul_bytes": nuls,
            },
        )
        r.line("INFO", "wrote", str(self.dir / "precheck.json"))

    def report_target(self, target: dict[str, Any], *, clear_schema: bool) -> bool:
        r = self.report
        ok = True
        for key in ("host", "db", "user"):
            ok &= r.check(
                bool(target.get(key)),
                f"target POSTGRES_{key.upper()}",
                target.get(key) or "not set in the container (WP4 not deployed?)",
            )
        ok &= r.check(
            target.get("password_set", False),
            "target POSTGRES_PASSWORD",
            "present" if target.get("password_set") else "absent",
        )
        if not ok:
            return False
        if not r.check(
            bool(target.get("reachable")),
            "target reachable",
            target.get("error")
            or f"{target.get('server_version')} as {target.get('current_user')}, "
            f"schema {target.get('schema')}, port {target.get('port')}",
        ):
            return False
        ok &= r.check(
            str(target.get("encoding", "")).upper() in {"UTF8", "UTF-8"},
            "target encoding UTF8",
            str(target.get("encoding")),
        )
        ok &= r.check(
            bool(target.get("can_create")),
            "target user may create tables",
            f"in schema {target.get('schema')}",
        )
        prefixed = int(target.get("prefixed_tables") or 0)
        if clear_schema:
            r.line(
                "INFO",
                "target tables",
                f"{prefixed} oc_ tables, dropped by --clear-schema",
            )
        else:
            ok &= r.check(
                prefixed == 0,
                "target is empty",
                f"{prefixed} oc_ tables, {target.get('other_tables')} other"
                + ("" if prefixed == 0 else "; a retry needs --clear-schema"),
            )
        return ok

    def report_inventory(self, inventory: dict[str, Any]) -> None:
        r = self.report
        db = inventory["db"]
        counts = table_counts(inventory)
        r.line(
            "INFO",
            "SQLite",
            f"{db['size']} bytes (wal {db['wal_size']}), journal {db['journal_mode']}, "
            f"sqlite {db['sqlite_version']}, {db['freelist_count']} free pages",
        )
        r.line(
            "INFO",
            "tables",
            f"{len(counts)} tables, {sum(counts.values())} rows",
        )
        p = inventory["prefix"]
        maxima = max_ids(inventory)
        named = {
            "max fileid": maxima.get(f"{p}filecache.fileid"),
            "shares": counts.get(f"{p}share"),
            "users (database backend)": counts.get(f"{p}users"),
            "accounts": counts.get(f"{p}accounts"),
            "user_oidc users": counts.get(f"{p}user_oidc"),
            "authtoken rows": counts.get(f"{p}authtoken"),
            "storages": counts.get(f"{p}storages"),
            "mounts": counts.get(f"{p}mounts"),
        }
        r.line("INFO", "facts", ", ".join(f"{k} {v}" for k, v in named.items()))
        folders = {t: n for t, n in counts.items() if t.startswith(f"{p}group_folders")}
        r.line(
            "INFO",
            "group folders",
            ", ".join(f"{t} {n}" for t, n in sorted(folders.items())) or "none",
        )
        providers = inventory.get("oidc_providers", [])
        r.line(
            "INFO",
            "user_oidc providers",
            ", ".join(f"{x['id']}={x['identifier']}" for x in providers) or "none",
        )
        above = marks_above_max(inventory)
        r.line(
            "INFO",
            "AUTOINCREMENT high-water above max id",
            f"{len(above)} of {len(inventory.get('sqlite_sequence') or {})} tables"
            + (
                ": "
                + ", ".join(f"{t} {hw} > {top}" for t, hw, top in above[:10])
                + " (convert raises these sequences)"
                if above
                else ""
            ),
        )

    # ------------------------------------------------------------------ close
    def close(self) -> None:
        r = self.report
        facts = self.facts()
        if facts["dbtype"] != "sqlite3":
            raise MigrationError(f"dbtype is {facts['dbtype']}: nothing to close for")
        self.advisory()
        status = self.status()
        previous = read_json(self.dir / "close.json")
        if status.get("maintenance") and previous:
            since = parse_iso(previous["maintenance_on_at"])
            r.line(
                "INFO",
                "maintenance",
                f"already on since {previous['maintenance_on_at']}",
            )
        else:
            self.kube.occ("maintenance:mode", "--on")
            since = utc_now()
            write_json(self.dir / "close.json", {"maintenance_on_at": iso(since)})
            r.line("PASS", "maintenance:mode --on", iso(since))
        remaining = self.args.settle_seconds - (utc_now() - since).total_seconds()
        if remaining > 0:
            r.line("INFO", "waiting for opcache", f"{int(remaining) + 1} s")
            self.sleep(remaining + 1)
        status = self.status()
        r.check(bool(status.get("maintenance")), "maintenance is on")
        self.no_cron_running(wait=self.args.cron_wait_seconds)
        print(CLOSE_CHECKLIST, flush=True)

    # ----------------------------------------------------------------- backup
    def backup(self) -> None:
        r = self.report
        hot = self.args.hot
        out = self.dir / "hot" if hot else self.dir
        out.mkdir(mode=0o700, parents=True, exist_ok=True)
        status = self.status()
        facts = self.facts()
        if facts["dbtype"] != "sqlite3":
            raise MigrationError(f"dbtype is {facts['dbtype']}: no SQLite to back up")
        if hot:
            r.line(
                "WARN", "hot copy", "maintenance not required; NOT a migration backup"
            )
        else:
            if not status.get("maintenance"):
                raise MigrationError(
                    "maintenance is off: run close first (or --hot for a rehearsal)"
                )
            self.maintenance_settled()
            self.no_cron_running()
        datadir = str(facts["datadirectory"])
        dbname = str(facts["dbname"] or "owncloud")
        sizes = self.db_files(datadir, dbname)
        db = f"{dbname}.db"
        if db not in sizes:
            raise MigrationError(f"{db} is not in {datadir}")
        names = sorted(sizes, key=lambda n: (n != db, n))
        before = self.pod_md5(datadir, names)
        # Compressed in the pod: a SQLite file is mostly free pages after a
        # cleanup, and a long exec stream can stall or end early (dev,
        # 2026-10-17: a 306 MB plain stream died at 120 MB over WebSockets and
        # ended short without an error over SPDY). The md5 checks below catch a
        # damaged copy either way; gzip makes an intact one likely.
        tar_path = out / "backup.tar.gz"
        argv = self.kube.exec_argv(
            f"tar -czf - -C {shlex.quote(datadir)} "
            + " ".join(shlex.quote(n) for n in names)
            + f" -C {NC_ROOT}/config config.php"
        )
        code, err = self.kube.stream_to_file(argv, tar_path)
        if code and not (hot and code == 1):
            raise MigrationError(
                f"tar over kubectl exec failed (exit {code}): {err[-400:]}"
            )
        if code:
            r.line("WARN", "tar", f"exit {code} (a file changed while read: hot copy)")
        after = self.pod_md5(datadir, names)
        files = out / "files"
        if files.exists():
            shutil.rmtree(files)
        files.mkdir(mode=0o700)
        expected = set(names) | {"config.php"}
        try:
            with tarfile.open(tar_path) as archive:
                for member in archive.getmembers():
                    if member.name not in expected or not member.isfile():
                        raise MigrationError(f"unexpected tar member {member.name!r}")
                    source = archive.extractfile(member)
                    assert source is not None
                    target = files / member.name
                    fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
                    with os.fdopen(fd, "wb") as handle:
                        shutil.copyfileobj(source, handle)
        except (tarfile.TarError, EOFError, OSError) as exc:
            raise MigrationError(
                f"{tar_path.name} is incomplete or damaged ({exc}): the transfer "
                "ended early; run backup again"
            ) from exc
        local = {name: md5_file(files / name) for name in sorted(expected)}
        verified = True
        for name in sorted(expected):
            same_pod = before.get(name) == after.get(name)
            same_local = local[name] == after.get(name) == before.get(name)
            if name.endswith("-shm"):
                # The wal-index is rebuilt from the WAL; readers write to it.
                r.line("INFO", f"md5 {name}", "wal-index, not compared")
                continue
            if same_local:
                r.line(
                    "PASS", f"md5 {name}", f"{local[name]} (pod before, after, local)"
                )
            elif hot:
                r.line(
                    "WARN",
                    f"md5 {name}",
                    "changed during the copy (live instance): "
                    + ("pod before != after" if not same_pod else "local != pod"),
                )
                verified = False
            else:
                r.line(
                    "FAIL",
                    f"md5 {name}",
                    f"pod before {before.get(name)}, after {after.get(name)}, local {local[name]}",
                )
                verified = False
        # The pod's database is read through SQLite's own locking, so even on
        # a live instance a failure here is real damage, not a torn copy.
        integrity_pod = self.kube.php(_PHP_INTEGRITY)
        verified &= r.check(
            integrity_pod["result"] == ["ok"],
            "integrity_check in the pod",
            "; ".join(integrity_pod["result"][:3]),
        )
        local_result = self.local_integrity(files, names)
        local_ok = local_result == ["ok"]
        if hot and not local_ok:
            r.line(
                "WARN", "integrity_check of the local copy", "; ".join(local_result[:3])
            )
            verified = False
        else:
            verified &= r.check(
                local_ok,
                "integrity_check of the local copy",
                "; ".join(local_result[:3]),
            )
        inventory = self.kube.php(_PHP_SQLITE_INVENTORY)
        self.report_inventory(inventory)
        samples = self.pick_samples(facts)
        manifest = {
            "mode": "hot" if hot else "maintenance",
            "label": "hot copy, not a migration backup" if hot else "migration backup",
            "created_at": iso(utc_now()),
            "context": self.kube.context,
            "namespace": self.kube.namespace,
            "deployment": self.kube.deployment,
            "datadirectory": datadir,
            "dbname": dbname,
            "files": {
                name: {
                    "size": (files / name).stat().st_size,
                    "md5_pod_before": before.get(name),
                    "md5_pod_after": after.get(name),
                    "md5_local": local[name],
                }
                for name in sorted(expected)
            },
            "integrity_pod": integrity_pod["result"],
            "integrity_local": local_result,
            "digests": facts["digests"],
            "version": status.get("versionstring"),
            "samples": samples,
            "sqlite_sequence": inventory.get("sqlite_sequence", {}),
            "verified": bool(verified) and not hot,
        }
        write_json(out / "inventory.json", inventory)
        write_json(out / "backup.json", manifest)
        if hot:
            r.line(
                "INFO",
                "result",
                f"hot copy, not a migration backup ({'consistent' if verified else 'inconsistent'}) "
                f"in {out}",
            )
        else:
            r.check(verified, "backup verified", str(out))

    def local_integrity(self, files: Path, names: list[str]) -> list[str]:
        check = files.parent / "integrity-check"
        if check.exists():
            shutil.rmtree(check)
        check.mkdir(mode=0o700)
        try:
            for name in names:
                if name.endswith("-shm"):
                    continue
                shutil.copy2(files / name, check / name)
            db = next(n for n in names if n.endswith(".db"))
            conn = sqlite3.connect(check / db)
            try:
                rows = [row[0] for row in conn.execute("PRAGMA integrity_check")]
            finally:
                conn.close()
            return rows[:20]
        except sqlite3.DatabaseError as exc:
            return [f"error: {exc}"]
        finally:
            shutil.rmtree(check, ignore_errors=True)

    def pick_samples(self, facts: dict[str, Any]) -> list[dict[str, Any]]:
        r = self.report
        if facts.get("objectstore_multibucket"):
            r.line("WARN", "sample files", "multibucket object store: skipped")
            return []
        result = self.kube.php(
            _PHP_SAMPLES, {"mode": "pick", "max_bytes": SAMPLE_MAX_BYTES}
        )
        samples = []
        for s in result["samples"]:
            if s.get("md5"):
                samples.append(s)
                r.line(
                    "PASS",
                    f"sample file {s['fileid']}",
                    f"{s['storage']}, {s['size']} bytes, read via {s['via']}",
                )
            else:
                r.line(
                    "WARN",
                    f"sample file {s.get('fileid')}",
                    s.get("error", "unreadable"),
                )
        if not samples:
            r.line(
                "WARN", "sample files", "none readable: verify cannot prove file reads"
            )
        return samples

    def manifest(self) -> dict[str, Any]:
        manifest = read_json(self.dir / "backup.json")
        if not manifest:
            raise MigrationError(f"no backup.json in {self.dir}: run backup first")
        if manifest.get("mode") != "maintenance" or not manifest.get("verified"):
            raise MigrationError(
                "backup.json is not a verified maintenance-mode backup"
            )
        if (
            manifest.get("namespace") != self.kube.namespace
            or manifest.get("context") != self.kube.context
        ):
            raise MigrationError("backup.json belongs to another context or namespace")
        return manifest

    # ---------------------------------------------------------------- convert
    def convert(self) -> None:
        r = self.report
        facts = self.facts()
        if facts["dbtype"] != "sqlite3":
            raise MigrationError(
                f"dbtype is already {facts['dbtype']}: run verify (or rollback)"
            )
        if not self.status().get("maintenance"):
            raise MigrationError("maintenance is off: run close, then backup")
        self.maintenance_settled()
        manifest = self.manifest()
        self.no_cron_running()
        datadir, dbname = manifest["datadirectory"], manifest["dbname"]
        sizes = self.db_files(datadir, dbname)
        names = sorted(sizes, key=lambda n: (not n.endswith(".db"), n))
        now = self.pod_md5(datadir, names)
        db = f"{dbname}.db"
        recorded = manifest["files"]
        same_db = now.get(db) == recorded[db]["md5_local"]
        same_wal = effective_wal(now.get(f"{db}-wal")) == effective_wal(
            recorded.get(f"{db}-wal", {}).get("md5_local")
        )
        if not r.check(
            same_db and same_wal,
            "database unchanged since the backup",
            "md5 equal" if same_db and same_wal else "it changed: run backup again",
        ):
            raise MigrationError("the database changed after the backup")
        if not self.report_target(self.target(), clear_schema=self.args.clear_schema):
            raise MigrationError("the target database is not ready")
        shell = convert_shell(
            clear_schema=self.args.clear_schema, chunk_size=self.args.chunk_size
        )
        r.line(
            "INFO", "converting", "db:convert-type --all-apps (output in convert.log)"
        )
        started = utc_now()
        result = self.kube.run(self.kube.exec_argv(shell), timeout=None, check=False)
        output = _text(result.stdout) + _text(result.stderr)
        log = self.dir / "convert.log"
        fd = os.open(log, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(output)
        lines = [ln.strip() for ln in re.split(r"[\r\n]+", output) if ln.strip()]
        tables = [ln for ln in lines if ln.startswith("- ")]
        r.line(
            "INFO",
            "convert-type",
            f"exit {result.returncode}, {len(tables)} lines naming a table or app",
        )
        skipped = _skipped_tables(lines)
        if skipped:
            r.line(
                "FAIL",
                "tables not converted",
                ", ".join(skipped) + " (verify will show them missing; roll back)",
            )
        for ln in lines[-8:]:
            if not re.match(r"^\d+/\d+ \[", ln):
                print(f"  | {ln[:200]}", flush=True)
        after = self.facts()
        status = self.status()
        r.check(bool(status.get("maintenance")), "maintenance is on again")
        converted = after["dbtype"] == "pgsql"
        if result.returncode == 0:
            r.check(converted, "dbtype is pgsql", str(after["dbtype"]))
        else:
            r.line(
                "FAIL",
                "convert-type failed",
                f"exit {result.returncode}; config.php still says {after['dbtype']} "
                "(a failed convert-type never writes it). Fix the cause and run convert "
                "again with --clear-schema, or reopen on SQLite with "
                "occ maintenance:mode --off",
            )
        raised: list[dict[str, Any]] = []
        if converted:
            # convert-type resyncs every sequence to max(id)+1, but SQLite's
            # AUTOINCREMENT never reuses an id, deleted rows' included. Raise
            # them now, while maintenance still keeps every client out.
            inventory = read_json(self.dir / "inventory.json") or {}
            source = self.pod_high_water(f"{datadir.rstrip('/')}/{dbname}.db")
            raised = self.raise_sequences(merge_high_water(inventory, source))
        write_json(
            self.dir / "convert.json",
            {
                "started_at": iso(started),
                "finished_at": iso(utc_now()),
                "exit": result.returncode,
                "dbtype": after["dbtype"],
                "clear_schema": self.args.clear_schema,
                "skipped_tables": skipped,
                "sequences": raised,
            },
        )
        if converted:
            print(
                "Next: restart the pod (clears opcache) and verify, still in "
                "maintenance: the verify phase with --restart.",
                flush=True,
            )

    # ------------------------------------------------------- sequences
    def pod_high_water(self, path: str) -> dict[str, Any] | None:
        """sqlite_sequence of the SQLite file in the pod (``None``: none there)."""
        try:
            return self.kube.php(_PHP_SQLITE_SEQUENCE, {"path": path})
        except MigrationError as exc:
            if "no SQLite database at" in str(exc):
                return None
            raise

    def raise_sequences(self, marks: dict[str, int]) -> list[dict[str, Any]]:
        entries = self.kube.php(_PHP_RAISE_SEQUENCES, {"high_water": marks})[
            "sequences"
        ]
        for item in raise_findings(entries, marks):
            self.report.finding(item)
        return entries

    def fix_sequences(self) -> None:
        """Repair an install converted without the raise: idempotent, raises only."""
        r = self.report
        facts = self.facts()
        if facts["dbtype"] != "pgsql":
            raise MigrationError(f"dbtype is {facts['dbtype']}: nothing to repair")
        manifest = read_json(self.dir / "backup.json") or {}
        sources: list[dict[str, Any] | None] = []
        used = []
        inventory = read_json(self.dir / "inventory.json")
        if inventory and inventory.get("sqlite_sequence"):
            sources.append(inventory)
            used.append("inventory.json")
        local = local_high_water(self.dir / "files")
        if local is not None:
            sources.append(local)
            used.append("the local backup copy")
        path = self.args.sqlite_path or (
            f"{manifest['datadirectory'].rstrip('/')}/{manifest['dbname']}.db"
            if manifest.get("datadirectory")
            else f"{str(facts['datadirectory']).rstrip('/')}/nextcloud.db"
        )
        pod = self.pod_high_water(path)
        if pod is not None:
            sources.append(pod)
            used.append(f"{path} in the pod")
        if not used:
            raise MigrationError(
                "no SQLite high-water marks: no inventory.json with sqlite_sequence, "
                f"no local backup copy in {self.dir / 'files'}, no {path} in the pod "
                "(--sqlite-path)"
            )
        marks = merge_high_water(*sources)
        r.line(
            "INFO", "high-water marks", f"{len(marks)} tables from {', '.join(used)}"
        )
        entries = self.raise_sequences(marks)
        write_json(
            self.dir / "fix-sequences.json",
            {"at": iso(utc_now()), "sources": used, "sequences": entries},
        )

    # ----------------------------------------------------------------- verify
    def restart(self) -> None:
        d = f"deployment/{self.kube.deployment}"
        self.kube.kubectl("rollout", "restart", d)
        self.report.line("INFO", "rollout restart", d)
        self.kube.kubectl(
            "rollout", "status", d, f"--timeout={ROLLOUT_TIMEOUT}", timeout=1000
        )
        for _ in range(30):
            try:
                self.status()
                break
            except MigrationError:
                self.sleep(5)
        else:
            raise MigrationError("occ status does not answer after the restart")
        self.report.line("PASS", "restarted", d)
        logs = self.kube.kubectl(
            "logs", d, "-c", self.kube.container, "--since=30m", timeout=120
        )
        for line in logs.splitlines():
            if "[nc-setup]" in line:
                print(f"  | {line.strip()[:200]}", flush=True)

    def verify(self) -> None:
        r = self.report
        manifest = self.manifest()
        facts = self.facts()
        if not r.check(
            facts["dbtype"] == "pgsql", "dbtype is pgsql", str(facts["dbtype"])
        ):
            raise MigrationError("not on Postgres: nothing to verify")
        # The data first, through PDO (the CLI has no opcache): a restart
        # runs the before-starting hook, which re-applies app settings.
        self.verify_data(manifest)
        if self.args.restart:
            self.restart()
        facts = self.facts()
        status = self.status()
        r.check(facts["dbtype"] == "pgsql", "dbtype is pgsql", str(facts["dbtype"]))
        r.check(
            bool(status.get("installed")), "installed", status.get("versionstring", "")
        )
        r.check(not status.get("needsDbUpgrade"), "no pending upgrade")
        if not status.get("maintenance"):
            r.line(
                "WARN",
                "maintenance is off",
                "the instance is open: writes may already be on Postgres",
            )
        for key, digest in manifest["digests"].items():
            r.check(
                facts["digests"].get(key) == digest and digest is not None,
                f"{key} unchanged",
                "digest equal"
                if facts["digests"].get(key) == digest
                else "DIGEST DIFFERS",
            )
        self.verify_samples(manifest.get("samples", []))
        passed = not self.report.failures
        write_json(
            self.dir / "verify.json",
            {
                "at": iso(utc_now()),
                "passed": passed,
                "restarted": bool(self.args.restart),
                "failures": self.report.failures,
                "backup_created_at": manifest["created_at"],
            },
        )
        r.check(
            passed, "verify", "go/no-go: open, or rollback" if passed else "do NOT open"
        )

    def verify_data(self, manifest: dict[str, Any]) -> None:
        r = self.report
        inventory = read_json(self.dir / "inventory.json")
        if not inventory:
            raise MigrationError("no inventory.json beside backup.json")
        prefix = inventory.get("prefix", "oc_")
        # Postgres must equal the SQLite file as convert-type copied it; that
        # file may differ from the backup only by convert-type's own writes.
        source = self.kube.php(
            _PHP_SQLITE_INVENTORY,
            {
                "path": f"{manifest['datadirectory'].rstrip('/')}/{manifest['dbname']}.db"
            },
        )
        for item in source_drift(inventory, source, prefix):
            r.finding(item)
        pg = self.kube.php(_PHP_PG_INVENTORY, {"idcols": id_columns(source)})
        r.line(
            "INFO", "Postgres", f"{pg['server_version']}, {len(pg['tables'])} tables"
        )
        marks = merge_high_water(inventory, source)
        if not inventory.get("sqlite_sequence"):
            r.line(
                "WARN",
                "high-water marks",
                "the backup predates recording sqlite_sequence; using the SQLite "
                "file as convert-type left it",
            )
        for item in compare_inventories(source, pg, prefix, high_water=marks):
            r.finding(item)
        r.check(
            pg.get("encryption_enabled") == inventory.get("encryption_enabled"),
            "server-side encryption unchanged",
            str(pg.get("encryption_enabled")),
        )
        r.check(
            pg.get("oidc_providers") == inventory.get("oidc_providers"),
            "user_oidc providers unchanged",
            ", ".join(
                f"{x['id']}={x['identifier']}" for x in pg.get("oidc_providers", [])
            )
            or "none",
        )

    def verify_samples(self, samples: list[dict[str, Any]]) -> None:
        r = self.report
        if not samples:
            r.line(
                "WARN", "sample files", "none recorded at backup: read a file by hand"
            )
            return
        result = self.kube.php(
            _PHP_SAMPLES,
            {
                "mode": "check",
                "samples": [
                    {"storage": s["storage"], "path": s["path"]} for s in samples
                ],
            },
        )
        for old, new in zip(samples, result["samples"]):
            label = f"sample file {old['fileid']}"
            if new.get("missing"):
                r.line("FAIL", label, "not in the Postgres filecache")
                continue
            same = (
                new.get("fileid") == old["fileid"]
                and new.get("etag") == old["etag"]
                and new.get("md5") == old["md5"]
            )
            r.check(
                same,
                label,
                f"fileid, etag and content equal, read via {new.get('via')}"
                if same
                else f"fileid {new.get('fileid')}, etag "
                f"{'equal' if new.get('etag') == old['etag'] else 'differs'}, "
                f"content {'equal' if new.get('md5') == old['md5'] else new.get('error') or 'differs'}",
            )

    # ------------------------------------------------------------------- open
    def open(self) -> None:
        r = self.report
        manifest = self.manifest()
        verified = read_json(self.dir / "verify.json")
        if not verified or not verified.get("passed"):
            raise MigrationError("no passing verify.json: run verify first")
        if verified.get("backup_created_at") != manifest["created_at"]:
            raise MigrationError("verify.json is from an earlier backup: verify again")
        converted = read_json(self.dir / "convert.json") or {}
        if not verified.get("restarted"):
            # Apache and the FPM sidecar may serve a config.php compiled
            # before the conversion (still SQLite) until opcache revalidates.
            finished = converted.get("finished_at")
            settled = (
                finished is not None
                and (utc_now() - parse_iso(finished)).total_seconds()
                >= SETTLE_MIN_SECONDS
            )
            if not settled:
                raise MigrationError(
                    "verify ran without --restart and the conversion is under "
                    f"{SETTLE_MIN_SECONDS} s old: run verify --restart"
                )
            r.line("WARN", "no restart", "opcache has revalidated since the conversion")
        facts = self.facts()
        if facts["dbtype"] != "pgsql":
            raise MigrationError(f"dbtype is {facts['dbtype']}, not pgsql")
        self.kube.occ("maintenance:mode", "--off")
        r.line("PASS", "maintenance:mode --off", iso(utc_now()))
        write_json(self.dir / "open.json", {"opened_at": iso(utc_now())})
        for command in (
            "db:add-missing-indices",
            "db:add-missing-columns",
            "db:add-missing-primary-keys",
        ):
            result = self.kube.run(
                self.kube.occ_argv(command), check=False, timeout=3600
            )
            r.check(
                result.returncode == 0,
                command,
                (_text(result.stdout).strip().splitlines() or [""])[-1][:200],
            )
        status = self.status()
        r.check(not status.get("maintenance"), "maintenance is off")
        print(SMOKE_CHECKLIST, flush=True)
        print(
            DB_BACKUP_REMINDER.format(
                namespace=self.kube.namespace,
                context=self.kube.context,
                backup_dir=self.dir,
            ),
            flush=True,
        )
        print(OPEN_NOTES, flush=True)

    # --------------------------------------------------------------- rollback
    def rollback(self) -> None:
        r = self.report
        override = self.args.i_know_maintenance_was_off
        if (self.dir / "open.json").exists() and not override:
            raise MigrationError(
                "open.json exists: the instance was opened on Postgres, the point of "
                "no return has passed (override: --i-know-maintenance-was-off)"
            )
        manifest = self.manifest()
        status = self.status()
        if not status.get("maintenance") and not override:
            raise MigrationError(
                "maintenance is off: writes may have landed on Postgres already "
                "(override: --i-know-maintenance-was-off)"
            )
        facts = self.facts()
        if facts["dbtype"] == "sqlite3":
            r.line(
                "PASS",
                "dbtype is sqlite3",
                "config.php was never switched: nothing to restore; reopen with "
                "occ maintenance:mode --off",
            )
            return
        config = self.dir / "files" / "config.php"
        content = config.read_bytes()
        if (
            hashlib.md5(content).hexdigest()
            != manifest["files"]["config.php"]["md5_local"]
        ):
            raise MigrationError("the local config.php does not match backup.json")
        argv = self.kube.exec_argv(
            f"umask 027 && cat > {NC_ROOT}/config/config.php.srw-rollback"
            f" && mv -f {NC_ROOT}/config/config.php.srw-rollback {NC_ROOT}/config/config.php",
            stdin=True,
        )
        self.kube.run(argv, data=content)
        pod = self.kube.sh(f"md5sum -- {NC_ROOT}/config/config.php").split()[0]
        if not r.check(
            pod == manifest["files"]["config.php"]["md5_local"],
            "config.php restored",
            pod,
        ):
            raise MigrationError("the restored config.php does not match the backup")
        write_json(self.dir / "rollback.json", {"restored_at": iso(utc_now())})
        self.restart()
        facts = self.facts()
        r.check(facts["dbtype"] == "sqlite3", "dbtype is sqlite3", str(facts["dbtype"]))
        status = self.status()
        r.line("INFO", "maintenance", "on" if status.get("maintenance") else "off")
        sizes = self.db_files(manifest["datadirectory"], manifest["dbname"])
        db = f"{manifest['dbname']}.db"
        now = self.pod_md5(manifest["datadirectory"], sorted(sizes))
        if now.get(db) == manifest["files"][db]["md5_local"]:
            r.line("PASS", "SQLite since the backup", "unchanged")
        else:
            inventory = read_json(self.dir / "inventory.json") or {}
            current = self.kube.php(_PHP_SQLITE_INVENTORY)
            for item in source_drift(
                inventory, current, inventory.get("prefix", "oc_")
            ):
                # The instance runs on this file now: report, do not fail.
                r.line(
                    "WARN" if item.level == "FAIL" else item.level,
                    item.label,
                    item.detail,
                )
        print(
            "Next: check the instance, then reopen it with occ maintenance:mode --off.\n"
            "The Postgres database still holds the converted copy: a later convert "
            "needs --clear-schema.",
            flush=True,
        )


def _skipped_tables(lines: list[str]) -> list[str]:
    """Tables convert-type announced it would not convert."""
    skipped: list[str] = []
    collecting = False
    for line in lines:
        if "will not be converted" in line:
            collecting = True
            continue
        if collecting:
            if re.fullmatch(r"[A-Za-z0-9_]+", line):
                skipped.append(line)
            else:
                collecting = False
    return skipped


# --------------------------------------------------------------------------
# Entry point
# --------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__.split("\n\n", 1)[0],
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__.split("\n\n", 1)[1],
    )
    parser.add_argument("phase", choices=PHASES)
    parser.add_argument("--context", required=True, help="kube context (no default)")
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--deployment", default="srw-nextcloud")
    parser.add_argument("--container", default="nextcloud")
    parser.add_argument("--backup-dir", required=True)
    parser.add_argument("--orchestrator-deployment", default="srw-orchestrator")
    parser.add_argument("--orchestrator-container", default="orchestrator")
    parser.add_argument("--run", action="store_true")
    parser.add_argument("--confirm")
    parser.add_argument("--confirm-open")
    parser.add_argument(
        "--hot", action="store_true", help="backup: read-only rehearsal"
    )
    parser.add_argument("--clear-schema", action="store_true", help="convert: a retry")
    parser.add_argument("--chunk-size", type=int, help="convert: rows per query")
    parser.add_argument("--restart", action="store_true", help="verify: restart first")
    parser.add_argument("--settle-seconds", type=int, default=DEFAULT_SETTLE_SECONDS)
    parser.add_argument(
        "--sqlite-path",
        help="fix-sequences: the old SQLite file in the pod (default: from "
        "backup.json, else <datadirectory>/nextcloud.db)",
    )
    parser.add_argument("--cron-container", default="cron")
    parser.add_argument(
        "--cron-wait-seconds",
        type=int,
        default=600,
        help="close: how long to wait for a running cron.php to end",
    )
    parser.add_argument("--i-know-maintenance-was-off", action="store_true")
    return parser


def print_plan(args: argparse.Namespace) -> None:
    print(
        f"Phase {args.phase} on context {args.context}, namespace {args.namespace}, "
        f"deploy/{args.deployment} -c {args.container}; backup dir {args.backup_dir}"
    )
    if args.phase == "open":
        print(POINT_OF_NO_RETURN)
    for step in PLANS[args.phase]:
        print(f"  {step}")
    if args.phase == "convert":
        print("The shell line (run as www-data in the container):")
        for line in convert_shell(
            clear_schema=args.clear_schema, chunk_size=args.chunk_size
        ).splitlines():
            print(f"    {line}")
    flags = f"--run --confirm {confirm_token(args.namespace)}"
    if args.phase == "open":
        flags += f" --confirm-open {open_token(args.namespace)}"
    print(f"Dry run: nothing was executed. Add {flags}.")


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    error = gate_error(args)
    if error:
        print(error, file=sys.stderr)
        return 2
    backup_dir = Path(args.backup_dir).expanduser()
    if inside_git_worktree(backup_dir):
        print(
            "refusing a backup directory inside a git work tree: it will hold "
            "config.php, which carries secrets",
            file=sys.stderr,
        )
        return 2
    if not args.run:
        print_plan(args)
        return 0
    backup_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    kube = Kube(args.context, args.namespace, args.deployment, args.container)
    migration = Migration(args, kube)
    print(
        f"{args.phase} on {args.context}/{args.namespace} deploy/{args.deployment} "
        f"at {iso(utc_now())}",
        flush=True,
    )
    try:
        getattr(migration, args.phase.replace("-", "_"))()
    except MigrationError as exc:
        migration.report.line("FAIL", args.phase, str(exc))
    failures = migration.report.failures
    print(
        f"{'FAIL' if failures else 'PASS'} {args.phase}: "
        + (
            f"{len(failures)} failed: {', '.join(failures[:8])}"
            if failures
            else "all checks passed"
        ),
        flush=True,
    )
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
