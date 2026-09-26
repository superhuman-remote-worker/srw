"""Unused creation grants require immutable receipt and narrow SQL evidence."""

import json
from pathlib import Path
from uuid import uuid4

import asyncpg
import pytest
import pytest_asyncio

from tests.test_vm_creation_retry_real_postgres import (
    _schema_applied,  # noqa: F401
    admit,
    admitted_job,
    db as _db_fixture,
    pg_dsn,  # noqa: F401
    postgres_db_fixture,  # noqa: F401
)


db = _db_fixture
MIGRATIONS = Path(__file__).resolve().parents[1] / "src/orchestrator/database/migrations/app"
RECEIPT = b"r" * 32


def _function_ddl(filename, signature):
    source = (MIGRATIONS / filename).read_text()
    start = source.index(signature)
    end = source.index("\n$$;", start) + len("\n$$;")
    return source[start:end]


@pytest_asyncio.fixture
async def upgraded_conn(db, pg_dsn, request):  # noqa: F811
    job, generation, proposal = await admitted_job(db)
    retry = await admit(db, job, generation, proposal)
    conn = await asyncpg.connect(pg_dsn)
    tx = conn.transaction()
    await tx.start()
    try:
        # Recreate the predecessor on the disposable, current-schema database.
        await conn.execute(_function_ddl(
            "0258_vm_creation_effects.sql",
            "CREATE FUNCTION guard_vm_creation_effect_identity()",
        ).replace("CREATE FUNCTION", "CREATE OR REPLACE FUNCTION", 1))
        await conn.execute(_function_ddl(
            "0259_vm_creation_adoption.sql",
            "CREATE OR REPLACE FUNCTION guard_vm_creation_retry_identity()",
        ))
        await conn.execute(
            "DROP FUNCTION IF EXISTS public.valid_vm_creation_unused_grant_attention("
            "public.vm_creation_retries)"
        )
        await conn.execute(
            "ALTER TABLE public.vm_creation_effects "
            "DROP CONSTRAINT IF EXISTS vm_creation_issuer_receipt_sha256_length, "
            "DROP COLUMN IF EXISTS issuer_receipt_sha256"
        )
        historical_nonce = None
        if getattr(request, "param", False):
            historical_nonce = uuid4()
            await conn.execute(
                "INSERT INTO vm_creation_effects(effect_nonce,request_id,effect_number,"
                "effect_kind,carrier_uid,carrier_namespace,carrier_intent) "
                "VALUES($1,$2,1,'rootdisk',$3,'workers','{}'::jsonb)",
                historical_nonce, retry["request_id"], uuid4(),
            )
        for filename in (
            "0284_vm_creation_unused_grant_receipt.sql",
            "0285_validate_vm_creation_unused_grant_receipt.sql",
        ):
            sql = "\n".join(
                line for line in (MIGRATIONS / filename).read_text().splitlines()
                if line not in {"BEGIN;", "COMMIT;"}
            )
            await conn.execute(sql)
        assert await conn.fetchval(
            "SELECT convalidated FROM pg_constraint WHERE conname=$1",
            "vm_creation_issuer_receipt_sha256_length",
        ) is True
        yield conn, retry["request_id"], historical_nonce
    finally:
        await tx.rollback()
        await conn.close()


async def _insert_effect(conn, request_id, number, *, receipt=RECEIPT,
                         state="issued", evidence=None):
    nonce = uuid4()
    await conn.execute(
        "INSERT INTO vm_creation_effects(effect_nonce,request_id,effect_number,"
        "effect_kind,carrier_uid,carrier_namespace,carrier_intent,state,evidence,"
        "resolved_at,issuer_receipt_sha256) VALUES($1,$2,$3,'rootdisk',$4,'workers',"
        "'{}'::jsonb,$5,$6::jsonb,CASE WHEN $5='issued' THEN NULL ELSE "
        "clock_timestamp() END,$7)",
        nonce, request_id, number, uuid4(), state, json.dumps(evidence or {}), receipt,
    )
    return nonce


async def _rejects_check(conn, sql, *args):
    savepoint = conn.transaction()
    await savepoint.start()
    try:
        with pytest.raises(asyncpg.CheckViolationError):
            await conn.execute(sql, *args)
    finally:
        await savepoint.rollback()


@pytest.mark.asyncio
@pytest.mark.parametrize("upgraded_conn", [True], indirect=True)
async def test_upgrade_preserves_issued_predecessor_without_issuer_receipt(upgraded_conn):
    conn, request_id, historical_nonce = upgraded_conn
    effect = await conn.fetchrow(
        "SELECT request_id,state,issuer_receipt_sha256 FROM vm_creation_effects "
        "WHERE effect_nonce=$1", historical_nonce,
    )
    assert effect["request_id"] == request_id
    assert effect["state"] == "issued"
    assert effect["issuer_receipt_sha256"] is None
    await _rejects_check(
        conn, "UPDATE vm_creation_effects SET issuer_receipt_sha256=$2 "
        "WHERE effect_nonce=$1", historical_nonce, RECEIPT,
    )


@pytest.mark.asyncio
async def test_receipt_length_and_issued_or_resolved_identity(upgraded_conn):
    conn, request_id, _ = upgraded_conn
    old = await _insert_effect(conn, request_id, 1, receipt=None)
    assert await conn.fetchval(
        "SELECT issuer_receipt_sha256 FROM vm_creation_effects WHERE effect_nonce=$1", old,
    ) is None
    await _rejects_check(
        conn, "UPDATE vm_creation_effects SET issuer_receipt_sha256=$2 "
        "WHERE effect_nonce=$1", old, RECEIPT,
    )
    await conn.execute(
        "UPDATE vm_creation_effects SET state='rejected',"
        "evidence=$2::jsonb,resolved_at=clock_timestamp() WHERE effect_nonce=$1",
        old, json.dumps({"outcome": "api_rejected"}),
    )
    current = await _insert_effect(conn, request_id, 2)
    assert bytes(await conn.fetchval(
        "SELECT issuer_receipt_sha256 FROM vm_creation_effects WHERE effect_nonce=$1",
        current,
    )) == RECEIPT
    await _rejects_check(
        conn, "UPDATE vm_creation_effects SET issuer_receipt_sha256=$2 "
        "WHERE effect_nonce=$1", current, b"x" * 32,
    )
    await conn.execute(
        "UPDATE vm_creation_effects SET state='rejected',"
        "evidence=$2::jsonb,resolved_at=clock_timestamp() WHERE effect_nonce=$1",
        current, json.dumps({"outcome": "api_rejected"}),
    )
    await _rejects_check(
        conn, "UPDATE vm_creation_effects SET issuer_receipt_sha256=$2 "
        "WHERE effect_nonce=$1", current, b"x" * 32,
    )
    await _rejects_check(
        conn, "INSERT INTO vm_creation_effects(effect_nonce,request_id,effect_number,"
        "effect_kind,carrier_uid,carrier_namespace,carrier_intent,"
        "issuer_receipt_sha256) VALUES($1,$2,3,'rootdisk',$3,'workers','{}'::jsonb,$4)",
        uuid4(), request_id, uuid4(), b"x" * 31,
    )


@pytest.mark.asyncio
async def test_no_attempt_evidence_requires_receipt_exact_shape_and_allowlisted_reason(
    upgraded_conn,
):
    conn, request_id, _ = upgraded_conn
    historical = await _insert_effect(conn, request_id, 1, receipt=None)
    await _rejects_check(
        conn, "UPDATE vm_creation_effects SET state='rejected',"
        "evidence=$2::jsonb,resolved_at=clock_timestamp() WHERE effect_nonce=$1",
        historical, json.dumps({"outcome": "not_attempted", "reason": "resource_node_changed"}),
    )
    await conn.execute(
        "UPDATE vm_creation_effects SET state='rejected',"
        "evidence=$2::jsonb,resolved_at=clock_timestamp() WHERE effect_nonce=$1",
        historical, json.dumps({"outcome": "api_rejected"}),
    )
    current = await _insert_effect(conn, request_id, 2)
    for evidence in (
        {"outcome": "not_attempted"},
        {"outcome": "not_attempted", "reason": None},
        {"outcome": "not_attempted", "reason": "unknown"},
        {"outcome": "not_attempted", "reason": "resource_node_changed", "extra": True},
    ):
        await _rejects_check(
            conn, "UPDATE vm_creation_effects SET state='rejected',"
            "evidence=$2::jsonb,resolved_at=clock_timestamp() WHERE effect_nonce=$1",
            current, json.dumps(evidence),
        )
    await conn.execute(
        "UPDATE vm_creation_effects SET state='rejected',"
        "evidence=$2::jsonb,resolved_at=clock_timestamp() WHERE effect_nonce=$1",
        current, json.dumps({"outcome": "not_attempted", "reason": "resource_node_changed"}),
    )


@pytest.mark.asyncio
async def test_attention_requires_latest_receipted_terminal_nonretryable_proof(
    upgraded_conn,
):
    conn, request_id, _ = upgraded_conn
    attention = (
        "UPDATE vm_creation_retries SET state='attention',"
        "reason='vm_creation_retry_blocked',revision=revision+1,"
        "claim_token=NULL,claim_expires_at=NULL WHERE request_id=$1"
    )
    await _rejects_check(conn, attention, request_id)
    cases = (
        (None, "rejected", {"outcome": "not_attempted", "reason": "resource_node_changed"}),
        (RECEIPT, "issued", {}),
        (RECEIPT, "rejected", {"outcome": "api_rejected", "reason": "resource_node_changed"}),
        (RECEIPT, "rejected", {"outcome": "not_attempted", "reason": "resource_inventory_unavailable"}),
        (RECEIPT, "rejected", {"outcome": "not_attempted", "reason": "unknown"}),
        (RECEIPT, "rejected", {"outcome": "not_attempted"}),
        (RECEIPT, "rejected", {"outcome": "not_attempted", "reason": None}),
        (RECEIPT, "rejected", {"outcome": "not_attempted", "reason": "resource_node_changed", "extra": True}),
    )
    for receipt, state, evidence in cases:
        savepoint = conn.transaction()
        await savepoint.start()
        try:
            await _insert_effect(
                conn, request_id, 1, receipt=receipt, state=state, evidence=evidence,
            )
            await _rejects_check(conn, attention, request_id)
        finally:
            await savepoint.rollback()

    savepoint = conn.transaction()
    await savepoint.start()
    try:
        await _insert_effect(conn, request_id, 1, state="rejected", evidence={
            "outcome": "not_attempted", "reason": "resource_node_changed",
        })
        await _insert_effect(conn, request_id, 2, state="issued")
        await _rejects_check(conn, attention, request_id)
    finally:
        await savepoint.rollback()

    for reason in (
        "resource_node_changed", "creation_carrier_changed",
        "creation_observed_object_missing", "creation_observed_object_changed",
        "retained_disk_changed", "workspace_recovery_held",
        "workspace_attachment_unproven", "creation_existing_vm_unproven",
        "creation_rootdisk_source_unproven",
    ):
        savepoint = conn.transaction()
        await savepoint.start()
        try:
            await _insert_effect(conn, request_id, 1, state="rejected", evidence={
                "outcome": "not_attempted", "reason": reason,
            })
            await conn.execute(attention, request_id)
            assert await conn.fetchval(
                "SELECT state FROM vm_creation_retries WHERE request_id=$1", request_id,
            ) == "attention"
        finally:
            await savepoint.rollback()
