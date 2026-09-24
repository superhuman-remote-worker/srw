"""List and detail queries expose the same bounded, current creation progress."""

import json

import pytest

from tests.test_vm_creation_resume_real_postgres import (
    db as _db_fixture,
    postgres_db_fixture,  # noqa: F401
    pg_dsn,  # noqa: F401
    _schema_applied,  # noqa: F401
    pending,
)
from tests.test_vm_creation_preflight_real_postgres import resolving
from tests.test_job_projection import redact
from orchestrator.services.vm_creation_retry_store import VMCreationRetryStore

db = _db_fixture


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["preflight", "ledger"])
async def test_real_list_and_detail_show_same_attention_without_private_history(
    db, monkeypatch, stage
):
    monkeypatch.setenv("VM_CREATION_RETRY_ENABLED", "true")
    job, original = await pending(db, stage)
    detail = redact(await db.get_job(str(job)))
    page = await db.query_jobs(limit=10)
    listed = redact(next(row for row in page.jobs if str(row["id"]) == str(job)))
    assert listed["vm_creation"] == detail["vm_creation"]
    assert listed["vm_creation"]["request_id"] == original["request_id"]
    assert listed["vm_creation"]["resumable"] is True
    for result in (listed, detail):
        wire = json.dumps(result, default=str)
        assert "creation_preflight" not in wire
        assert "controller_configuration" not in wire
        assert "_vm_creation_pending" not in wire
        assert "_vm_creation" not in result


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "outcome,reason,source,expected",
    [
        ("capacity_wait", "capacity_wait", None, "controller_count_wait"),
        ("capacity_wait", "installation_budget", None, "capacity_wait"),
        ("capacity_wait", "resource_wait", "resource_admission", "resource_wait"),
        ("capacity_wait", "resource_unavailable", "resource_admission", "resource_unavailable"),
        ("dependency_wait", "preparation_wait", None, "preparation_wait"),
        ("dependency_wait", "SECRET raw body", None, "creation_dependency_pending"),
        ("transport_unknown", "SECRET raw body", None, "controller_unavailable"),
    ],
)
async def test_observer_persists_only_bounded_diagnostic_category(
    db, outcome, reason, source, expected
):
    job, preflight, claim, resolved = await resolving(db)
    await preflight.complete_resolution(claim, resolved)
    store = VMCreationRetryStore(db)
    row = (await store.claim_due(limit=1))[0]
    observation = {"outcome": outcome, "reason": reason}
    if source is not None:
        observation["source"] = source
    assert await store.apply_observation(
        request_id=str(row["request_id"]),
        claim_token=str(row["claim_token"]),
        expected_revision=row["revision"],
        observation=observation,
    )
    result = redact(await db.get_job(str(job)))
    assert result["vm_creation"]["reason_code"] == expected
    if reason == "installation_budget":
        assert result["vm_creation"]["wait"]["kind"] == "unknown"
    assert result["vm_creation"]["resumable"] is False
    async with db.acquire() as conn:
        assert (
            await conn.fetchval(
                "SELECT reason FROM vm_creation_retries WHERE job_id=$1", job
            )
            == expected
        )
    assert "SECRET" not in json.dumps(result, default=str)


@pytest.mark.asyncio
async def test_malformed_preflight_cannot_break_the_job_list(db):
    job, _ = await pending(db, "preflight")
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET context=jsonb_set(context,'{vm,creation_preflight,state}','[]') WHERE id=$1",
            job,
        )
    page = await db.query_jobs(limit=10)
    assert any(str(row["id"]) == str(job) for row in page.jobs)
    for row in page.jobs:
        assert "_vm_creation" not in redact(row)


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["preflight", "ledger"])
async def test_cleanup_precedes_creation_in_list_and_detail(db, monkeypatch, stage):
    monkeypatch.setenv("VM_CREATION_RETRY_ENABLED", "true")
    job, _ = await pending(db, stage)
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET status='paused',context=jsonb_set(context,'{vm,status}','\"retiring_process_zero\"') WHERE id=$1",
            job,
        )
    detail = redact(await db.get_job(str(job)))
    page = await db.query_jobs(limit=10)
    listed = redact(next(row for row in page.jobs if str(row["id"]) == str(job)))
    assert listed["error_message"] == detail["error_message"]
    assert "cleanup" in listed["error_message"].lower()
    assert "retained" in listed["error_message"].lower()
    for result in (detail, listed):
        assert result["vm_creation"]["resumable"] is False
