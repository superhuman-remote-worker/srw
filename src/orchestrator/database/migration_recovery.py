"""Explicit compatibility and recovery contracts for historical migrations.

Successful migration history normally requires a byte-exact file checksum.
A published edit can exceptionally have the same durable effects as the
original file. Such compatibility must pin the filename and both exact hashes;
it never rewrites the ledger, replays applied SQL, or accepts a failed row.

A successful row can also name a file that was published under another name.
That compatibility pins the historical filename, the canonical filename and
the one exact checksum both share; it only keeps the historical row from
counting as a missing file. The row stays as recorded, and the canonical file
is ordinary: it applies at its own position under its own name.

Non-transactional DDL can commit its physical side effect before the migration
ledger is updated, or leave an unusable catalog object behind when it fails.
Most such failures still need operator judgement.  The small registry below is
only for migrations whose cleanup, replay prerequisite, and final catalog shape
have all been reviewed as safe to retry automatically.

Keeping these contracts outside the immutable SQL files lets an already-applied
migration gain a recovery path without changing its checksum.  It also avoids
trying to infer safety by parsing arbitrary SQL.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class AppliedChecksumCompatibility:
    """One reviewed historical checksum for an exact canonical SQL file."""

    canonical_checksum: str
    historical_checksum: str


# 097b414f2 inserted only comments and a session-local pgvector input cast into
# already-applied vector 0025. Its CREATE FUNCTION bytes and durable effects are
# identical to the original from 734250425 (also shipped in 99169725a). Restore
# the original file; migrate._load_installed_pgvector already loads the library
# on cold upgrade connections. Databases that successfully applied the edited
# artifact retain their actual historical checksum, timestamps, and attribution.
# This exception is directional: only the original exact bytes are canonical.
APPLIED_CHECKSUM_COMPATIBILITIES: dict[str, AppliedChecksumCompatibility] = {
    "0025_knowledge_multi_angle_search.sql": AppliedChecksumCompatibility(
        canonical_checksum=(
            "fc9395f5ca2fd60a48932d8038e92238124ae23d7174d558d22a89a83d791ab8"
        ),
        historical_checksum=(
            "915246808c5714610aeb98faac61d96b5a2a72a81cba26677ba2d9b636325424"
        ),
    ),
    # The R3.2 capture migration (see RENAMED_APPLIED_MIGRATIONS below) was
    # renumbered locally with a rewritten two-line header comment; its SQL
    # statement is byte-identical. On 2026-09-28 the local k3d database applied
    # that variant as 0301 after its two historical rows had been deleted by
    # hand. Only the bytes first applied anywhere (131dd22ee, as 0286) are
    # canonical; that database keeps its row as recorded and nothing replays.
    "0301_capture_claimless_retired_agent_pod.sql": AppliedChecksumCompatibility(
        canonical_checksum=(
            "a2d08b52d9197d52e43da0859bf328be91c16fbb94feea31c1cdf2e1694bac2e"
        ),
        historical_checksum=(
            "587ed9b5edc56bd4946cf0637c679eaba1484ce5237da7f45b1873542fe838e5"
        ),
    ),
}


@dataclass(frozen=True)
class RenamedAppliedMigration:
    """One reviewed successful ledger row whose exact file has a new name."""

    canonical_filename: str
    checksum: str


# R3.2 wrote these two app migrations as 0286/0287. The local k3d development
# database applied them successfully under those names (source 131dd22ee,
# checksums below) before upstream published its own 0286-0300, and they are
# published as 0301/0302 with the same bytes. Each file is one
# CREATE OR REPLACE FUNCTION of an existing trigger function (0224's
# capture_retired_pinned_agent_pod, 0200's
# validate_thread_agent_warm_binding_protection) and changes no data; upstream's
# 0286-0300 neither define those functions nor write through their triggers,
# so the historical order leaves the same result. On that database the historical rows
# stay exactly as recorded; the canonical files still apply after upstream's
# 0286-0300, re-running these reviewed bytes once so the function bodies,
# ledger and migration head match every other installation. Their replay keeps
# each function's identity, owner, privileges and trigger bindings (see
# tests/test_claimless_retired_agent_pod_upgrade_real_postgres.py). Only the
# exact name and checksum below are accepted; a failed row is not.
RENAMED_APPLIED_MIGRATIONS: dict[str, RenamedAppliedMigration] = {
    "0286_capture_claimless_retired_agent_pod.sql": RenamedAppliedMigration(
        canonical_filename="0301_capture_claimless_retired_agent_pod.sql",
        checksum="a2d08b52d9197d52e43da0859bf328be91c16fbb94feea31c1cdf2e1694bac2e",
    ),
    "0287_permanent_retirement_releases_warm_protection.sql": RenamedAppliedMigration(
        canonical_filename="0302_permanent_retirement_releases_warm_protection.sql",
        checksum="c8df370587940ecebc93c0c53a4ff48e29e1d761018fb5b78e318321f2be6d8f",
    ),
}


@dataclass(frozen=True)
class ConcurrentIndexRecovery:
    """Reviewed recovery recipe for one ``CREATE INDEX CONCURRENTLY`` file."""

    cleanup_filename: str
    replay_filename: str
    index_name: str
    table_name: str
    access_method: str
    key_definitions: tuple[str, ...]
    predicate: str


# 0130 is deliberately replay-safe: it deterministically retires duplicate
# losers and asserts that none remain.  0131 is deliberately replay-safe: its
# exact DROP INDEX CONCURRENTLY removes either an INVALID shell or an unexpected
# same-name shape.  Those reviewed properties are what make 0132 recoverable;
# do not add an entry here merely because a migration happens to create an
# index concurrently.
NOTX_RECOVERIES: dict[str, ConcurrentIndexRecovery] = {
    "0132_jobs_verification_uniq.notx.sql": ConcurrentIndexRecovery(
        cleanup_filename="0131_drop_jobs_verification_uniq.notx.sql",
        replay_filename="0130_jobs_verification_dedupe.sql",
        index_name="jobs_verification_uniq",
        table_name="jobs",
        access_method="btree",
        key_definitions=(
            "parent_job_id",
            "(context ->> 'verification_round'::text)",
        ),
        predicate=(
            "(((context ->> 'verification_target'::text) IS NOT NULL) "
            "AND jsonb_exists(context, 'verification_round'::text))"
        ),
    )
}
