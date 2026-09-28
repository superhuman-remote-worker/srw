"""Explicit compatibility and recovery contracts for historical migrations.

Successful migration history normally requires a byte-exact file checksum.
A published edit can exceptionally have the same durable effects as the
original file. Such compatibility must pin the filename and both exact hashes;
it never rewrites the ledger, replays applied SQL, or accepts a failed row.

A successful row can also name a file that was published under another name.
That compatibility pins the historical filename, the canonical filename, the
canonical checksum and the exact historical checksums reviewed as the same
statement; it only keeps the historical row from counting as a missing file.
The row stays as recorded, and the canonical file is ordinary: it applies at
its own position under its own name.

A successful row can finally name an unpublished file whose effect published
migrations replaced. That contract pins the historical filename and checksum
and the exact superseding files; while those are on disk with their reviewed
bytes, the row is retained unchanged and nothing replays on its behalf.

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
}


@dataclass(frozen=True)
class RenamedAppliedMigration:
    """One reviewed successful ledger row whose exact statement has a new name."""

    canonical_filename: str
    checksum: str
    historical_checksums: tuple[str, ...]


# R3.2's claim-less retired-Pod capture (one CREATE OR REPLACE of 0224's
# capture_retired_pinned_agent_pod trigger function; no data) has carried
# three names. The local k3d development database applied it as 0286 from
# 131dd22ee (2026-09-27), and after a manual ledger repair again as 0301 from
# 50d0af34a, whose copy differed only in a rewritten two-line header comment
# (587ed9b5...). Local develop 9270a8aa1 published the original bytes as 0301.
# Upstream then took 0301-0305, so the statement is published as 0306 with the
# bytes first applied (a2d08b52...). Historical rows stay exactly as recorded
# and the canonical file applies at its own position, re-running the same
# statement once: upstream's 0286-0305 neither define that function nor write
# through its trigger, so the replay keeps its identity, owner, privileges and
# trigger binding (tests/test_claimless_retired_agent_pod_upgrade_real_postgres.py).
# Only these names and checksums are accepted; a failed row is not.
RENAMED_APPLIED_MIGRATIONS: dict[str, RenamedAppliedMigration] = {
    "0286_capture_claimless_retired_agent_pod.sql": RenamedAppliedMigration(
        canonical_filename="0306_capture_claimless_retired_agent_pod.sql",
        checksum="a2d08b52d9197d52e43da0859bf328be91c16fbb94feea31c1cdf2e1694bac2e",
        historical_checksums=(
            "a2d08b52d9197d52e43da0859bf328be91c16fbb94feea31c1cdf2e1694bac2e",
        ),
    ),
    "0301_capture_claimless_retired_agent_pod.sql": RenamedAppliedMigration(
        canonical_filename="0306_capture_claimless_retired_agent_pod.sql",
        checksum="a2d08b52d9197d52e43da0859bf328be91c16fbb94feea31c1cdf2e1694bac2e",
        historical_checksums=(
            "a2d08b52d9197d52e43da0859bf328be91c16fbb94feea31c1cdf2e1694bac2e",
            "587ed9b5edc56bd4946cf0637c679eaba1484ce5237da7f45b1873542fe838e5",
        ),
    ),
}


@dataclass(frozen=True)
class SupersededAppliedMigration:
    """One reviewed successful ledger row whose effect published files replaced."""

    checksum: str
    superseded_by: tuple[tuple[str, str], ...]


# R3.2's warm-release validator (0200's validate_thread_agent_warm_binding_protection
# with a `releasing` branch accepted at a deleted owner) was applied locally as
# 0287 (131dd22ee) and as 0302 (50d0af34a, 9270a8aa1), never published. Upstream's
# 0301 fixed the same defect with a fenced `terminal_release` state, and its text
# replacement requires 0200's `releasing` branch. 0300z restores 0200's body
# on exactly this variant before 0301 runs; replaying the R3.2 file after 0301
# would delete the `terminal_release` branch. The row is retained unchanged,
# never replayed, and accepted only while both superseding files are on disk
# with these bytes.
_R32_WARM_VALIDATOR_SUPERSEDED_BY = (
    (
        "0300z_restore_upstream_warm_binding_validator.sql",
        "6591dde15e2619e592579795cb670f111469d940143eeeed3aaf88a7155594ad",
    ),
    (
        "0301_pinned_permanent_warm_release.sql",
        "8a07a05bcf8aeb26a463d85b5a64cfd55f3c8a8f135d477b6067cef687abe9f9",
    ),
)
SUPERSEDED_APPLIED_MIGRATIONS: dict[str, SupersededAppliedMigration] = {
    "0287_permanent_retirement_releases_warm_protection.sql": (
        SupersededAppliedMigration(
            checksum="c8df370587940ecebc93c0c53a4ff48e29e1d761018fb5b78e318321f2be6d8f",
            superseded_by=_R32_WARM_VALIDATOR_SUPERSEDED_BY,
        )
    ),
    "0302_permanent_retirement_releases_warm_protection.sql": (
        SupersededAppliedMigration(
            checksum="c8df370587940ecebc93c0c53a4ff48e29e1d761018fb5b78e318321f2be6d8f",
            superseded_by=_R32_WARM_VALIDATOR_SUPERSEDED_BY,
        )
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
