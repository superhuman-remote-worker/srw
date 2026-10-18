"""The SQLite -> Postgres runbook script for the bundled Nextcloud (never run here).

Covers the parts that decide safety without a cluster: the confirm-token
gate, the dry run (nothing executed), the declared-length parsing behind the
VARCHAR pre-check, the inventory comparison behind verify, and that no
command line the script builds carries a credential.
"""

from __future__ import annotations

import importlib.util
import io
import json
import sqlite3
import subprocess
import sys
import tarfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]
_SCRIPT = ROOT / "scripts" / "nextcloud-sqlite-to-postgres.py"
_SPEC = importlib.util.spec_from_file_location("nextcloud_sqlite_to_postgres", _SCRIPT)
assert _SPEC is not None and _SPEC.loader is not None
mig = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = mig
_SPEC.loader.exec_module(mig)

#: Values a credential could have on the operator's machine.
SENTINELS = {
    "POSTGRES_PASSWORD": "pg-sentinel-Secret-1",
    "NEXTCLOUD_ADMIN_PASSWORD": "admin-sentinel-Secret-2",
    "NEXTCLOUD_DB_PASSWORD": "db-sentinel-Secret-3",
    "SRW_SESSION_TOKEN": "token-sentinel-Secret-4",
}


def _args(phase: str, backup_dir: Path, *extra: str):
    return mig.build_parser().parse_args(
        [
            phase,
            "--context",
            "k3d-srw",
            "--namespace",
            "srw",
            "--backup-dir",
            str(backup_dir),
            *extra,
        ]
    )


def _inventory(**tables):
    return {"prefix": "oc_", "tables": tables, "oidc_providers": []}


def _table(rows, *columns):
    return {"rows": rows, "columns": list(columns)}


# --------------------------------------------------------------------------
# VARCHAR pre-check
# --------------------------------------------------------------------------


class TestDeclaredLength:
    @pytest.mark.parametrize(
        ("declared", "limit"),
        [
            ("VARCHAR(64)", 64),
            ("varchar(4000)", 4000),
            ("VARCHAR ( 255 )", 255),
            ("CHAR(32)", 32),
            ("NCHAR(5)", 5),
            ("CHARACTER(7)", 7),
            ("CHARACTER VARYING(10)", 10),
            ("NVARCHAR(12)", 12),
        ],
    )
    def test_length_types(self, declared, limit):
        assert mig.declared_length(declared) == limit

    @pytest.mark.parametrize(
        "declared",
        [
            "",
            None,
            "CLOB",
            "TEXT",
            "BLOB",
            "INTEGER",
            "BIGINT UNSIGNED",
            "VARCHAR",
            "NUMERIC(10,2)",
            "DOUBLE PRECISION",
            "DATETIME",
        ],
    )
    def test_unbounded_types(self, declared):
        assert mig.declared_length(declared) is None

    def test_overruns_name_only_columns_past_their_limit(self):
        inventory = _inventory(
            oc_share=_table(
                3,
                {"name": "token", "type": "VARCHAR(32)", "pk": 0, "maxlen": 33},
                {"name": "share_with", "type": "VARCHAR(255)", "pk": 0, "maxlen": 255},
                {"name": "note", "type": "CLOB", "pk": 0, "maxlen": 90000},
                {"name": "label", "type": "VARCHAR(255)", "pk": 0, "maxlen": None},
            ),
            oc_users=_table(
                1, {"name": "uid", "type": "VARCHAR(64)", "pk": 1, "maxlen": 5}
            ),
        )
        assert mig.varchar_overruns(inventory) == [
            {
                "table": "oc_share",
                "column": "token",
                "declared": "VARCHAR(32)",
                "limit": 32,
                "max_length": 33,
            }
        ]

    def test_nul_bytes_are_reported_per_column(self):
        inventory = _inventory(
            oc_comments=_table(
                9,
                {
                    "name": "message",
                    "type": "CLOB",
                    "pk": 0,
                    "maxlen": 10,
                    "nul_rows": 2,
                },
                {"name": "actor_id", "type": "VARCHAR(64)", "pk": 0, "nul_rows": 0},
            )
        )
        assert mig.nul_findings(inventory) == [
            {"table": "oc_comments", "column": "message", "rows": 2}
        ]


# --------------------------------------------------------------------------
# verify: counts, max ids, sequences
# --------------------------------------------------------------------------


def _sqlite_inventory():
    return _inventory(
        oc_filecache=_table(
            100, {"name": "fileid", "type": "INTEGER", "pk": 1, "max": 21337}
        ),
        oc_share=_table(118, {"name": "id", "type": "INTEGER", "pk": 1, "max": 400}),
        oc_migrations=_table(
            500, {"name": "app", "type": "VARCHAR(255)", "pk": 1, "maxlen": 20}
        ),
    )


def _postgres(**overrides):
    pg = {
        "tables": {"oc_filecache": 100, "oc_share": 118, "oc_migrations": 520},
        "max": {"oc_filecache.fileid": 21337, "oc_share.id": 400},
        "sequences": [
            {
                "table": "oc_filecache",
                "column": "fileid",
                "sequence": "public.oc_filecache_fileid_seq",
                "last_value": 21337,
                "is_called": True,
                "increment": 1,
                "max": 21337,
            }
        ],
    }
    pg.update(overrides)
    return pg


def _levels(findings):
    return {f.label: f.level for f in findings}


class TestCompareInventories:
    def test_a_faithful_copy_passes(self):
        findings = mig.compare_inventories(_sqlite_inventory(), _postgres())
        assert not [f for f in findings if f.level == "FAIL"], findings
        levels = _levels(findings)
        assert levels["row counts"] == "PASS"
        assert levels["max ids"] == "PASS"
        assert levels["sequences past max id and SQLite high-water"] == "PASS"
        # --all-apps adds migration rows of apps never enabled: not a failure.
        assert levels["table oc_migrations"] == "INFO"

    def test_a_count_mismatch_fails(self):
        pg = _postgres(
            tables={"oc_filecache": 99, "oc_share": 118, "oc_migrations": 500}
        )
        levels = _levels(mig.compare_inventories(_sqlite_inventory(), pg))
        assert levels["table oc_filecache"] == "FAIL"
        assert levels["row counts"] == "FAIL"

    def test_a_table_convert_type_skipped_fails(self):
        pg = _postgres(tables={"oc_filecache": 100, "oc_migrations": 500})
        levels = _levels(mig.compare_inventories(_sqlite_inventory(), pg))
        assert levels["table oc_share"] == "FAIL"

    def test_fewer_migration_rows_fail(self):
        pg = _postgres(
            tables={"oc_filecache": 100, "oc_share": 118, "oc_migrations": 7}
        )
        levels = _levels(mig.compare_inventories(_sqlite_inventory(), pg))
        assert levels["table oc_migrations"] == "FAIL"

    def test_tables_only_on_postgres_are_information(self):
        pg = _postgres(
            tables={
                "oc_filecache": 100,
                "oc_share": 118,
                "oc_migrations": 500,
                "oc_ldap_user_mapping": 0,
            }
        )
        levels = _levels(mig.compare_inventories(_sqlite_inventory(), pg))
        assert levels["tables only on Postgres"] == "INFO"
        assert levels["row counts"] == "PASS"

    @pytest.mark.parametrize("value", [21336, None, "error: column does not exist"])
    def test_a_different_max_id_fails(self, value):
        pg = _postgres(max={"oc_filecache.fileid": value, "oc_share.id": 400})
        levels = _levels(mig.compare_inventories(_sqlite_inventory(), pg))
        assert levels["max ids"] == "FAIL"

    @pytest.mark.parametrize(
        ("last_value", "is_called", "level"),
        [
            (21337, True, "PASS"),  # setval(max): next is max + 1
            (21338, False, "PASS"),
            (21337, False, "FAIL"),  # next would hand out max again
            (1, False, "FAIL"),  # never resynced
        ],
    )
    def test_sequences_must_be_past_their_max(self, last_value, is_called, level):
        sequence = dict(
            _postgres()["sequences"][0], last_value=last_value, is_called=is_called
        )
        pg = _postgres(sequences=[sequence])
        levels = _levels(mig.compare_inventories(_sqlite_inventory(), pg))
        assert levels["sequences past max id and SQLite high-water"] == level

    def test_an_empty_table_sequence_passes(self):
        sequence = dict(
            _postgres()["sequences"][0], last_value=1, is_called=False, max=None
        )
        levels = _levels(
            mig.compare_inventories(
                _sqlite_inventory(), _postgres(sequences=[sequence])
            )
        )
        assert levels["sequences past max id and SQLite high-water"] == "PASS"

    def test_next_sequence_value(self):
        assert mig.next_sequence_value(10, True) == 11
        assert mig.next_sequence_value(10, False) == 10
        assert mig.next_sequence_value(10, True, 5) == 15

    def test_settings_and_mount_cache_may_grow_after_a_restart(self):
        source = _sqlite_inventory()
        source["tables"]["oc_appconfig"] = _table(157)
        source["tables"]["oc_mounts"] = _table(
            0, {"name": "id", "type": "INTEGER", "pk": 1, "max": None}
        )
        tables = {"oc_filecache": 100, "oc_share": 118, "oc_migrations": 500}
        pg = _postgres(
            tables={**tables, "oc_appconfig": 158, "oc_mounts": 3},
            max={"oc_filecache.fileid": 21337, "oc_share.id": 400, "oc_mounts.id": 3},
        )
        levels = _levels(mig.compare_inventories(source, pg))
        assert levels["table oc_appconfig"] == "WARN"
        assert levels["table oc_mounts"] == "WARN"
        assert levels["max id oc_mounts.id"] == "WARN"
        assert levels["row counts"] == "PASS"
        assert levels["max ids"] == "PASS"

    def test_settings_never_shrink(self):
        source = _sqlite_inventory()
        source["tables"]["oc_appconfig"] = _table(157)
        tables = {"oc_filecache": 100, "oc_share": 118, "oc_migrations": 500}
        pg = _postgres(tables={**tables, "oc_appconfig": 150})
        levels = _levels(mig.compare_inventories(source, pg))
        assert levels["table oc_appconfig"] == "FAIL"


class TestSourceDrift:
    """The SQLite file between the backup and the end of convert-type."""

    def _backup(self):
        inventory = _sqlite_inventory()
        inventory["tables"]["oc_appconfig"] = _table(153)
        inventory["tables"]["oc_mounts"] = _table(5)
        inventory["encryption_enabled"] = None
        return inventory

    def test_unchanged_passes(self):
        levels = _levels(mig.source_drift(self._backup(), self._backup()))
        assert levels == {"SQLite source since the backup": "PASS"}

    def test_convert_types_own_writes_are_expected(self):
        # Rehearsed on 31.0.14: --all-apps boots the disabled encryption app
        # (4 key-id rows) and the user mount cache is emptied.
        source = self._backup()
        source["tables"]["oc_appconfig"] = _table(157)
        source["tables"]["oc_mounts"] = _table(0)
        levels = _levels(mig.source_drift(self._backup(), source))
        assert levels["source oc_appconfig"] == "INFO"
        assert levels["source oc_mounts"] == "INFO"
        assert levels["SQLite source since the backup"] == "PASS"

    def test_any_other_change_fails(self):
        source = self._backup()
        source["tables"]["oc_share"] = _table(119)
        levels = _levels(mig.source_drift(self._backup(), source))
        assert levels["source oc_share"] == "FAIL"
        assert levels["SQLite source since the backup"] == "FAIL"

    def test_lost_settings_fail(self):
        source = self._backup()
        source["tables"]["oc_appconfig"] = _table(140)
        levels = _levels(mig.source_drift(self._backup(), source))
        assert levels["source oc_appconfig"] == "FAIL"

    def test_encryption_switched_on_fails(self):
        source = self._backup()
        source["encryption_enabled"] = "yes"
        levels = _levels(mig.source_drift(self._backup(), source))
        assert levels["source encryption_enabled"] == "FAIL"


class TestHighWater:
    """SQLite's AUTOINCREMENT never reuses an id; Postgres must not either."""

    def test_the_target_is_the_higher_of_both_marks(self):
        assert mig.sequence_target(51, 18) == 51  # deleted top rows
        assert mig.sequence_target(10, 18) == 18  # mark behind the rows
        assert mig.sequence_target(None, 18) == 18  # no AUTOINCREMENT
        assert mig.sequence_target(51, None) == 51  # every row deleted
        assert mig.sequence_target(None, None) == 0

    @pytest.mark.parametrize("last_value", [1, 17, 18, 19, 50, 51, 52, 300])
    @pytest.mark.parametrize("is_called", [True, False])
    @pytest.mark.parametrize("increment", [1, 5])
    @pytest.mark.parametrize("target", [0, 18, 51])
    def test_the_raise_only_ever_raises(self, last_value, is_called, increment, target):
        before = mig.next_sequence_value(last_value, is_called, increment)
        after = mig.raised_next_value(last_value, is_called, increment, target)
        assert after >= before
        assert after > target
        if before > target:
            assert after == before  # already above: untouched

    def test_marks_merge_to_the_highest_per_table(self):
        merged = mig.merge_high_water(
            {"sqlite_sequence": {"oc_share": 40, "OC_Mounts": 600}},
            None,
            {"sqlite_sequence": {"oc_share": 51, "oc_mounts": 669, "oc_x": None}},
            {},
        )
        assert merged == {"oc_share": 51, "oc_mounts": 669}

    def test_tables_whose_mark_is_above_their_max_id(self):
        inventory = _sqlite_inventory()
        inventory["tables"]["oc_group_folders"] = _table(
            4, {"name": "folder_id", "type": "INTEGER", "pk": 1, "max": 295}
        )
        inventory["tables"]["oc_emptied"] = _table(
            0, {"name": "id", "type": "INTEGER", "pk": 1, "max": None}
        )
        inventory["sqlite_sequence"] = {
            "oc_share": 51,  # below max id 400
            "oc_group_folders": 299,  # folders 296-299 deleted
            "oc_filecache": 21337,  # equal
            "oc_emptied": 9,  # every row deleted
        }
        assert mig.marks_above_max(inventory) == [
            ("oc_emptied", 9, None),
            ("oc_group_folders", 299, 295),
        ]

    def test_verify_fails_a_sequence_below_the_high_water_mark(self):
        # The k3d case: convert-type left oc_share at max(id)+1 = 19, but
        # SQLite had handed out ids up to 51; SRW still holds some of them.
        sequence = {
            "table": "oc_share",
            "column": "id",
            "sequence": "public.oc_share_id_seq",
            "last_value": 18,
            "is_called": True,
            "increment": 1,
            "max": 18,
        }
        levels = _levels(mig.sequence_findings([sequence], {"oc_share": 51}))
        assert levels["sequences past max id and SQLite high-water"] == "FAIL"
        raised = dict(sequence, last_value=51)
        levels = _levels(mig.sequence_findings([raised], {"oc_share": 51}))
        assert levels["sequences past max id and SQLite high-water"] == "PASS"

    def test_verify_uses_the_high_water_marks_it_is_given(self):
        pg = _postgres()
        levels = _levels(
            mig.compare_inventories(
                _sqlite_inventory(), pg, high_water={"oc_filecache": 21400}
            )
        )
        assert levels["sequences past max id and SQLite high-water"] == "FAIL"

    def test_a_mark_without_a_sequence_only_warns(self):
        levels = _levels(mig.sequence_findings([], {"oc_old_table": 7, "x_other": 3}))
        assert levels["high-water marks without a sequence"] == "WARN"
        assert levels["sequences past max id and SQLite high-water"] == "PASS"

    def test_raise_findings(self):
        entries = [
            {"table": "oc_share", "column": "id", "max": 18, "high_water": 51,
             "target": 51, "next_before": 19, "next_after": 52, "raised": True},
            {"table": "oc_filecache", "column": "fileid", "max": 120,
             "high_water": 90, "target": 120, "next_before": 121,
             "next_after": 121, "raised": False},
            {"table": "oc_jobs", "column": "id", "error": "lock timeout"},
        ]  # fmt: skip
        levels = _levels(
            mig.raise_findings(entries, {"oc_share": 51, "oc_gone": 4, "oc_jobs": 9})
        )
        assert levels["raised oc_share.id"] == "INFO"
        assert "raised oc_filecache.fileid" not in levels
        assert levels["sequence oc_jobs.id"] == "FAIL"
        assert levels["high-water marks without a sequence"] == "WARN"
        assert levels["sequences above max id and SQLite high-water"] == "FAIL"

    def test_the_local_backup_copy_yields_its_marks(self, tmp_path):
        files = tmp_path / "files"
        files.mkdir()
        conn = sqlite3.connect(files / "nextcloud.db")
        conn.execute("CREATE TABLE oc_share (id INTEGER PRIMARY KEY AUTOINCREMENT, x)")
        conn.executemany(
            "INSERT INTO oc_share (x) VALUES (?)", [(i,) for i in range(5)]
        )
        conn.execute("DELETE FROM oc_share WHERE id > 3")
        conn.commit()
        conn.close()
        before = (files / "nextcloud.db").read_bytes()
        assert mig.local_high_water(files) == {"sqlite_sequence": {"oc_share": 5}}
        assert (files / "nextcloud.db").read_bytes() == before
        assert not (tmp_path / "high-water-read").exists()
        assert mig.local_high_water(tmp_path / "missing") is None


class TestSmallHelpers:
    def test_a_missing_and_an_empty_wal_are_the_same(self):
        assert mig.effective_wal(None) is None
        assert mig.effective_wal(mig.EMPTY_MD5) is None
        assert mig.effective_wal("abc") == "abc"

    def test_skipped_tables_are_read_from_convert_type_output(self):
        lines = [
            "Creating schema in new database",
            "The following tables will not be converted:",
            "oc_old_app_table",
            "oc_other",
            "Please note that tables belonging to disabled (but not removed) apps",
            "- oc_filecache",
        ]
        assert mig._skipped_tables(lines) == ["oc_old_app_table", "oc_other"]

    def test_backup_dir_inside_a_git_work_tree_is_detected(self, tmp_path):
        (tmp_path / "repo" / ".git").mkdir(parents=True)
        assert mig.inside_git_worktree(tmp_path / "repo" / "nc-backup" / "deeper")
        assert not mig.inside_git_worktree(tmp_path / "elsewhere")


# --------------------------------------------------------------------------
# Confirm-token gate and dry run
# --------------------------------------------------------------------------


class TestGate:
    def test_a_dry_run_needs_no_token(self, tmp_path):
        assert mig.gate_error(_args("convert", tmp_path)) is None

    def test_run_needs_the_namespace_token(self, tmp_path):
        assert "--confirm MIGRATE-NEXTCLOUD-srw" in mig.gate_error(
            _args("precheck", tmp_path, "--run")
        )
        assert mig.gate_error(
            _args("precheck", tmp_path, "--run", "--confirm", "MIGRATE-NEXTCLOUD-other")
        )
        assert (
            mig.gate_error(
                _args(
                    "precheck", tmp_path, "--run", "--confirm", "MIGRATE-NEXTCLOUD-srw"
                )
            )
            is None
        )

    def test_open_needs_a_second_token(self, tmp_path):
        first = ["--run", "--confirm", "MIGRATE-NEXTCLOUD-srw"]
        assert "--confirm-open OPEN-NEXTCLOUD-srw" in mig.gate_error(
            _args("open", tmp_path, *first)
        )
        assert mig.gate_error(
            _args("open", tmp_path, *first, "--confirm-open", "MIGRATE-NEXTCLOUD-srw")
        )
        assert (
            mig.gate_error(
                _args("open", tmp_path, *first, "--confirm-open", "OPEN-NEXTCLOUD-srw")
            )
            is None
        )

    def test_close_refuses_a_settle_shorter_than_opcache(self, tmp_path):
        assert mig.gate_error(
            _args(
                "close",
                tmp_path,
                "--run",
                "--confirm",
                "MIGRATE-NEXTCLOUD-srw",
                "--settle-seconds",
                "30",
            )
        )

    def test_the_context_has_no_default(self, tmp_path):
        with pytest.raises(SystemExit):
            mig.build_parser().parse_args(
                ["precheck", "--namespace", "srw", "--backup-dir", str(tmp_path)]
            )

    @pytest.mark.parametrize("phase", mig.PHASES)
    def test_a_dry_run_executes_nothing(self, phase, tmp_path, monkeypatch, capsys):
        def refuse(*args, **kwargs):
            pytest.fail("a dry run must not execute anything")

        monkeypatch.setattr(subprocess, "run", refuse)
        monkeypatch.setattr(subprocess, "Popen", refuse)
        backup_dir = tmp_path / "nc-backup"
        argv = [phase, "--context", "k3d-srw", "--namespace", "srw"]
        assert mig.main([*argv, "--backup-dir", str(backup_dir)]) == 0
        out = capsys.readouterr().out
        assert "Dry run: nothing was executed" in out
        assert not backup_dir.exists()
        if phase == "open":
            assert "POINT OF NO RETURN" in out
            assert "--confirm-open OPEN-NEXTCLOUD-srw" in out

    def test_a_wrong_token_executes_nothing(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            subprocess, "run", lambda *a, **k: pytest.fail("must not run")
        )
        argv = ["convert", "--context", "k3d-srw", "--namespace", "srw"]
        argv += ["--backup-dir", str(tmp_path / "b"), "--run", "--confirm", "yes"]
        assert mig.main(argv) == 2

    def test_a_backup_dir_in_a_git_work_tree_is_refused(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            subprocess, "run", lambda *a, **k: pytest.fail("must not run")
        )
        (tmp_path / ".git").mkdir()
        argv = ["precheck", "--context", "k3d-srw", "--namespace", "srw"]
        assert mig.main([*argv, "--backup-dir", str(tmp_path / "backup")]) == 2


# --------------------------------------------------------------------------
# No credential on any command line
# --------------------------------------------------------------------------


class TestConvertShell:
    def test_the_password_goes_on_stdin_untrimmed(self):
        shell = mig.convert_shell()
        assert "printf '%s' \"$POSTGRES_PASSWORD\" | php occ db:convert-type" in shell
        assert 'echo "$POSTGRES_PASSWORD' not in shell
        assert "--password" not in shell
        assert '--port "$port"' in shell
        assert "--all-apps" in shell
        assert " -n " in shell
        assert '"$POSTGRES_USER" "$host" "$POSTGRES_DB"' in shell
        assert "--clear-schema" not in shell
        assert "--clear-schema" in mig.convert_shell(clear_schema=True)
        assert "--chunk-size=500" in mig.convert_shell(chunk_size=500)

    def test_maintenance_goes_back_on_whatever_convert_type_returned(self):
        lines = mig.convert_shell().splitlines()
        convert = next(i for i, ln in enumerate(lines) if "db:convert-type" in ln)
        assert lines[convert + 1] == "rc=$?"
        assert lines[convert + 2] == "php occ maintenance:mode --on"
        assert lines[-1] == 'exit "$rc"'
        assert "&& php occ maintenance:mode" not in "\n".join(lines)


class TestArgvGuard:
    def test_a_secret_value_on_a_command_line_is_refused(self):
        with pytest.raises(mig.MigrationError):
            mig.assert_argv_clean(
                ["kubectl", "exec", "--", "sh", "-c", "x pg-sentinel-Secret-1"],
                SENTINELS,
            )

    def test_short_or_unrelated_values_are_ignored(self):
        mig.assert_argv_clean(
            ["kubectl", "-n", "srw"], {"PATH": "/usr/bin:/bin", "API_KEY": "srw"}
        )


class FakeCluster:
    """A runner answering the script's kubectl calls; records every argv."""

    def __init__(self, *, maintenance=True, dbtype="sqlite3", db_md5="d" * 32):
        self.maintenance = maintenance
        self.dbtype = dbtype
        self.db_md5 = db_md5
        self.calls: list[list[str]] = []
        self.inputs: list[str | bytes | None] = []
        self.cron_runs = 0
        #: sqlite_sequence of the old SQLite file in the pod (None: gone).
        self.pod_marks: dict[str, int] | None = {"oc_share": 51, "oc_filecache": 90}
        #: Postgres sequences as convert-type leaves them: max(id)+1.
        self.sequences = [
            {"table": "oc_share", "column": "id", "sequence": "public.oc_share_id_seq",
             "last_value": 18, "is_called": True, "increment": 1, "max": 18},
            {"table": "oc_filecache", "column": "fileid",
             "sequence": "public.oc_filecache_fileid_seq",
             "last_value": 120, "is_called": True, "increment": 1, "max": 120},
        ]  # fmt: skip

    def __call__(self, argv, input=None, text=True, capture_output=True, timeout=None):
        self.calls.append(list(argv))
        self.inputs.append(input)
        shell = argv[-1] if "www-data" in argv else ""
        out = ""
        if "get" in argv and any(a.startswith("jsonpath=") for a in argv):
            out = "nextcloud cron nextcloud-protected-effect-fpm"
        elif "cron[.]php" in shell:
            assert argv[argv.index("-c") + 1] == "cron"
            out = f"{self.cron_runs}\n"
        elif "db:convert-type" in shell:
            self.dbtype = "pgsql"
            self.maintenance = True
        elif "php occ status" in shell:
            out = json.dumps(
                {
                    "installed": True,
                    "version": "31.0.14.1",
                    "versionstring": "31.0.14",
                    "maintenance": self.maintenance,
                    "needsDbUpgrade": False,
                }
            )
        elif "maintenance:mode --on" in shell:
            self.maintenance = True
        elif "maintenance:mode --off" in shell:
            self.maintenance = False
        elif "stat -c" in shell:
            out = "1000 nextcloud.db\n"
        elif "md5sum" in shell:
            out = f"{self.db_md5}  nextcloud.db\n{'c' * 32}  config.php\n"
        elif shell.endswith("&& php"):
            out = "RESULT " + json.dumps(self._php(input or ""))
        return subprocess.CompletedProcess(argv, 0, out, "")

    def _php(self, script):
        if "'digests' =>" in script:
            return {
                "dbtype": self.dbtype,
                "dbname": "nextcloud",
                "datadirectory": "/var/www/html/data",
                "prefix": "oc_",
                "maintenance": self.maintenance,
                "objectstore": False,
                "objectstore_multibucket": False,
                "digests": {"instanceid": "i", "secret": "s", "passwordsalt": "p"},
            }
        if "'password_set' =>" in script:
            return {
                "host": "srw-nextclouddb",
                "db": "nextcloud",
                "user": "nextcloud",
                "password_set": True,
                "port": 5432,
                "reachable": True,
                "server_version": "16.15",
                "encoding": "UTF8",
                "current_user": "nextcloud",
                "schema": "public",
                "can_create": True,
                "prefixed_tables": 0,
                "other_tables": 0,
            }
        payload = json.loads(script.split("<<<'SRWJSON'\n", 1)[1].split("\nSRWJSON")[0])
        if "emit(['path' => $INPUT['path']" in script:
            if self.pod_marks is None:
                return {
                    "error": f"RuntimeException: no SQLite database at {payload['path']}"
                }
            return {"path": payload["path"], "sqlite_sequence": self.pod_marks}
        if "IN EXCLUSIVE MODE" in script:
            # The in-pod raise rule, applied to the fake sequences.
            out = []
            for seq in self.sequences:
                mark = payload["high_water"].get(seq["table"])
                nxt = mig.next_sequence_value(
                    seq["last_value"], seq["is_called"], seq["increment"]
                )
                target = mig.sequence_target(mark, seq["max"])
                raised = nxt <= target
                if raised:
                    seq["last_value"], seq["is_called"] = target, True
                after = mig.next_sequence_value(
                    seq["last_value"], seq["is_called"], seq["increment"]
                )
                out.append(
                    {**{k: seq[k] for k in ("table", "column", "sequence", "max")},
                     "high_water": mark, "target": target, "next_before": nxt,
                     "increment": seq["increment"], "raised": raised,
                     "next_after": after}
                )  # fmt: skip
            return {"sequences": out}
        raise AssertionError("unexpected PHP script")


def _settled(backup_dir: Path):
    since = datetime.now(timezone.utc) - timedelta(seconds=120)
    mig.write_json(backup_dir / "close.json", {"maintenance_on_at": mig.iso(since)})


def _verified_backup(backup_dir: Path, db_md5="d" * 32):
    mig.write_json(
        backup_dir / "backup.json",
        {
            "mode": "maintenance",
            "verified": True,
            "context": "k3d-srw",
            "namespace": "srw",
            "created_at": mig.iso(datetime.now(timezone.utc)),
            "datadirectory": "/var/www/html/data",
            "dbname": "nextcloud",
            "files": {
                "nextcloud.db": {"md5_local": db_md5},
                "config.php": {"md5_local": "c" * 32},
            },
            "digests": {"instanceid": "i", "secret": "s", "passwordsalt": "p"},
            "samples": [],
        },
    )


def _migration(phase, backup_dir, cluster, *extra):
    args = _args(
        phase, backup_dir, "--run", "--confirm", "MIGRATE-NEXTCLOUD-srw", *extra
    )
    kube = mig.Kube(
        "k3d-srw",
        "srw",
        "srw-nextcloud",
        "nextcloud",
        runner=cluster,
        environ=SENTINELS,
    )
    return mig.Migration(args, kube, sleep=lambda seconds: None)


def _ran_convert(cluster):
    return any("db:convert-type" in part for argv in cluster.calls for part in argv)


class _StreamingPopen:
    """Popen stand-in for ``kubectl exec``: writes ``payload`` and exits 0."""

    def __init__(self, payload: bytes):
        self.payload = payload
        self.argvs: list[list[str]] = []

    def __call__(self, argv, stdout, stderr):
        self.argvs.append(list(argv))
        stdout.write(self.payload)
        return SimpleNamespace(communicate=lambda: (None, b""), returncode=0)


def _gzip_tar(members: dict[str, bytes]) -> bytes:
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as archive:
        for name, data in members.items():
            info = tarfile.TarInfo(name)
            info.size = len(data)
            archive.addfile(info, io.BytesIO(data))
    return buffer.getvalue()


class TestBackupStream:
    def _backup(self, tmp_path, payload):
        cluster = FakeCluster()
        _settled(tmp_path)
        popen = _StreamingPopen(payload)
        args = _args("backup", tmp_path, "--run", "--confirm", "MIGRATE-NEXTCLOUD-srw")
        kube = mig.Kube(
            "k3d-srw",
            "srw",
            "srw-nextcloud",
            "nextcloud",
            runner=cluster,
            popen=popen,
            environ=SENTINELS,
        )
        migration = mig.Migration(args, kube, sleep=lambda seconds: None)
        return migration, popen

    def test_the_pod_compresses_the_stream(self, tmp_path):
        # Dev, 2026-10-17: a 306 MB plain stream stalled at 120 MB; the file
        # was mostly free pages and came across as 2 MB of gzip.
        migration, popen = self._backup(tmp_path, b"")
        with pytest.raises(mig.MigrationError):
            migration.backup()
        assert any("tar -czf - " in part for part in popen.argvs[0])
        assert not (tmp_path / "backup.tar").exists()

    def test_a_stream_that_ends_early_fails_the_phase(self, tmp_path):
        # Dev, 2026-10-17: kubectl over SPDY ended a long stream short and
        # still exited 0. The cut archive must fail the phase by name, not
        # with a tarfile traceback.
        archive = _gzip_tar(
            {"nextcloud.db": bytes(range(256)) * 4096, "config.php": b"<?php"}
        )
        migration, _ = self._backup(tmp_path, archive[: len(archive) // 2])
        with pytest.raises(mig.MigrationError, match="incomplete or damaged"):
            migration.backup()


class TestConvertGating:
    def test_refuses_with_maintenance_off(self, tmp_path):
        cluster = FakeCluster(maintenance=False)
        _settled(tmp_path)
        _verified_backup(tmp_path)
        with pytest.raises(mig.MigrationError, match="maintenance is off"):
            _migration("convert", tmp_path, cluster).convert()
        assert not _ran_convert(cluster)

    def test_refuses_before_maintenance_settled(self, tmp_path):
        cluster = FakeCluster()
        mig.write_json(
            tmp_path / "close.json",
            {"maintenance_on_at": mig.iso(datetime.now(timezone.utc))},
        )
        _verified_backup(tmp_path)
        with pytest.raises(mig.MigrationError, match="opcache"):
            _migration("convert", tmp_path, cluster).convert()
        assert not _ran_convert(cluster)

    def test_refuses_without_a_verified_backup(self, tmp_path):
        cluster = FakeCluster()
        _settled(tmp_path)
        with pytest.raises(mig.MigrationError, match="backup"):
            _migration("convert", tmp_path, cluster).convert()
        hot = {
            "mode": "hot",
            "verified": False,
            "context": "k3d-srw",
            "namespace": "srw",
        }
        mig.write_json(tmp_path / "backup.json", hot)
        with pytest.raises(mig.MigrationError, match="verified"):
            _migration("convert", tmp_path, cluster).convert()
        assert not _ran_convert(cluster)

    def test_refuses_when_the_database_changed_after_the_backup(self, tmp_path):
        cluster = FakeCluster(db_md5="e" * 32)
        _settled(tmp_path)
        _verified_backup(tmp_path, db_md5="d" * 32)
        with pytest.raises(mig.MigrationError, match="changed after the backup"):
            _migration("convert", tmp_path, cluster).convert()
        assert not _ran_convert(cluster)

    def test_refuses_while_a_cron_run_is_still_writing(self, tmp_path):
        # Maintenance mode stops new cron.php runs, not one already running.
        cluster = FakeCluster()
        cluster.cron_runs = 1
        _settled(tmp_path)
        _verified_backup(tmp_path)
        with pytest.raises(mig.MigrationError, match="cron.php still running"):
            _migration("convert", tmp_path, cluster).convert()
        assert not _ran_convert(cluster)

    def test_refuses_an_already_converted_instance(self, tmp_path):
        cluster = FakeCluster(dbtype="pgsql")
        _settled(tmp_path)
        _verified_backup(tmp_path)
        with pytest.raises(mig.MigrationError, match="already pgsql"):
            _migration("convert", tmp_path, cluster).convert()
        assert not _ran_convert(cluster)

    def test_converts_with_no_credential_on_any_command_line(self, tmp_path):
        cluster = FakeCluster()
        _settled(tmp_path)
        _verified_backup(tmp_path)
        migration = _migration("convert", tmp_path, cluster)
        migration.convert()
        assert _ran_convert(cluster)
        assert not migration.report.failures
        for argv in cluster.calls:
            for part in argv:
                for secret in SENTINELS.values():
                    assert secret not in part
                assert "--password" not in part
        convert = next(a for a in cluster.calls if "db:convert-type" in a[-1])
        assert convert[:4] == ["kubectl", "--context=k3d-srw", "-n", "srw"]
        assert convert[-6:-1] == ["su", "-s", "/bin/sh", "www-data", "-c"]
        assert json.loads((tmp_path / "convert.json").read_text())["dbtype"] == "pgsql"

    def test_convert_raises_sequences_to_the_high_water_in_maintenance(
        self, tmp_path, capsys
    ):
        cluster = FakeCluster()
        _settled(tmp_path)
        _verified_backup(tmp_path)
        # The backup's marks and the post-convert file's: the higher counts.
        mig.write_json(
            tmp_path / "inventory.json", {"sqlite_sequence": {"oc_share": 60}}
        )
        migration = _migration("convert", tmp_path, cluster)
        migration.convert()
        assert not migration.report.failures
        share, filecache = cluster.sequences
        assert mig.next_sequence_value(share["last_value"], True, 1) == 61
        assert filecache["last_value"] == 120  # above its mark 90: untouched
        assert cluster.maintenance is True
        raise_call = next(
            i
            for i, data in enumerate(cluster.inputs)
            if "IN EXCLUSIVE MODE" in (data or "")
        )
        convert_call = next(
            i for i, a in enumerate(cluster.calls) if "db:convert-type" in a[-1]
        )
        assert raise_call > convert_call
        out = capsys.readouterr().out
        assert "INFO raised oc_share.id: next 19 -> 61" in out
        recorded = json.loads((tmp_path / "convert.json").read_text())["sequences"]
        assert [s["raised"] for s in recorded] == [True, False]

    def test_a_failed_convert_raises_nothing(self, tmp_path):
        cluster = FakeCluster()
        _settled(tmp_path)
        _verified_backup(tmp_path)
        real = cluster.__call__

        def failing(argv, **kwargs):
            result = real(argv, **kwargs)
            if "db:convert-type" in argv[-1]:
                cluster.dbtype = "sqlite3"
                return subprocess.CompletedProcess(argv, 7, "", "SQLSTATE[22001]")
            return result

        migration = _migration("convert", tmp_path, cluster)
        migration.kube.runner = failing
        migration.convert()
        assert "convert-type failed" in migration.report.failures
        assert not any("IN EXCLUSIVE MODE" in (d or "") for d in cluster.inputs)


class TestFixSequences:
    def test_raises_from_the_pod_file_and_is_idempotent(self, tmp_path, capsys):
        cluster = FakeCluster(dbtype="pgsql")
        migration = _migration("fix-sequences", tmp_path, cluster)
        migration.fix_sequences()
        assert not migration.report.failures
        assert cluster.sequences[0]["last_value"] == 51
        first = capsys.readouterr().out
        assert "INFO raised oc_share.id: next 19 -> 52" in first
        again = _migration("fix-sequences", tmp_path, cluster)
        again.fix_sequences()
        assert not again.report.failures
        second = capsys.readouterr().out
        assert "INFO raised" not in second
        assert "0 raised, 2 already above" in second
        assert cluster.sequences[0]["last_value"] == 51

    def test_uses_the_local_backup_copy_when_the_pod_file_is_gone(self, tmp_path):
        cluster = FakeCluster(dbtype="pgsql")
        cluster.pod_marks = None
        files = tmp_path / "files"
        files.mkdir()
        conn = sqlite3.connect(files / "nextcloud.db")
        conn.execute("CREATE TABLE oc_share (id INTEGER PRIMARY KEY AUTOINCREMENT, x)")
        conn.executemany(
            "INSERT INTO oc_share (x) VALUES (?)", [(i,) for i in range(70)]
        )
        conn.execute("DELETE FROM oc_share WHERE id > 18")
        conn.commit()
        conn.close()
        migration = _migration("fix-sequences", tmp_path, cluster)
        migration.fix_sequences()
        assert cluster.sequences[0]["last_value"] == 70

    def test_refuses_without_any_high_water_source(self, tmp_path):
        cluster = FakeCluster(dbtype="pgsql")
        cluster.pod_marks = None
        with pytest.raises(mig.MigrationError, match="no SQLite high-water marks"):
            _migration("fix-sequences", tmp_path, cluster).fix_sequences()
        assert not any("IN EXCLUSIVE MODE" in (d or "") for d in cluster.inputs)

    def test_refuses_on_sqlite(self, tmp_path):
        cluster = FakeCluster(dbtype="sqlite3")
        with pytest.raises(mig.MigrationError, match="nothing to repair"):
            _migration("fix-sequences", tmp_path, cluster).fix_sequences()


class TestOpenAndRollbackGating:
    def test_open_needs_a_passing_verify_of_this_backup(self, tmp_path):
        cluster = FakeCluster(dbtype="pgsql")
        _verified_backup(tmp_path)
        extra = ("--confirm-open", "OPEN-NEXTCLOUD-srw")
        with pytest.raises(mig.MigrationError, match="verify"):
            _migration("open", tmp_path, cluster, *extra).open()
        mig.write_json(
            tmp_path / "verify.json",
            {"passed": True, "backup_created_at": "2020-01-01T00:00:00Z"},
        )
        with pytest.raises(mig.MigrationError, match="earlier backup"):
            _migration("open", tmp_path, cluster, *extra).open()
        assert cluster.maintenance is True

    def test_open_needs_a_restart_or_settled_opcache(self, tmp_path):
        cluster = FakeCluster(dbtype="pgsql")
        _verified_backup(tmp_path)
        created = json.loads((tmp_path / "backup.json").read_text())["created_at"]
        mig.write_json(
            tmp_path / "verify.json",
            {"passed": True, "restarted": False, "backup_created_at": created},
        )
        now = mig.iso(datetime.now(timezone.utc))
        mig.write_json(tmp_path / "convert.json", {"finished_at": now})
        extra = ("--confirm-open", "OPEN-NEXTCLOUD-srw")
        with pytest.raises(mig.MigrationError, match="--restart"):
            _migration("open", tmp_path, cluster, *extra).open()
        assert cluster.maintenance is True

    def test_rollback_refuses_with_maintenance_off(self, tmp_path):
        cluster = FakeCluster(maintenance=False, dbtype="pgsql")
        _verified_backup(tmp_path)
        with pytest.raises(mig.MigrationError, match="maintenance is off"):
            _migration("rollback", tmp_path, cluster).rollback()
        assert not any("cat >" in argv[-1] for argv in cluster.calls)

    def test_rollback_refuses_after_open(self, tmp_path):
        cluster = FakeCluster(dbtype="pgsql")
        _verified_backup(tmp_path)
        mig.write_json(tmp_path / "open.json", {"opened_at": "2026-10-14T00:00:00Z"})
        with pytest.raises(mig.MigrationError, match="point of no return"):
            _migration("rollback", tmp_path, cluster).rollback()
        assert cluster.calls == []
