"""0340 never retroactively converts a genuinely issued Ready True parent."""

from pathlib import Path

import pytest

from tests.test_vm_job_retained_resume_real_postgres import (  # noqa: F401
    _base_db,
    _db_fixture,
    _pre_ssh_db,
    _retention_db,
    db as _resume_db,
    enabled,
    pg_dsn,
    postgres_db_fixture,
    pre_ssh_schema,
    retention_schema,
    resume_schema,
    whole_schema,
)
from tests.test_vm_ready_purge_cancel_replay_real_postgres import (  # noqa: F401
    _schema_applied,
)

db = _resume_db


@pytest.mark.asyncio
async def test_issued_true_parent_survives_upgrade_and_both_retention_gate_values(
    db, monkeypatch
):
    from tests import test_vm_ready_purge_cancel_replay_real_postgres as historical
    from orchestrator.services.vm_job_cancel_retention import acquire_cancel_retention

    state = await historical.ready_purge(db)
    path = (
        Path(__file__).resolve().parents[1]
        / "src/orchestrator/database/migrations/app/0340_vm_job_ready_cancel_retention.sql"
    )
    async with db.acquire() as conn:
        await conn.execute(path.read_text())
    for gate in ("false", "true"):
        monkeypatch.setenv("VM_JOB_CANCEL_RETENTION_ENABLED", gate)
        assert (
            await acquire_cancel_retention(
                state["recovery"], job_id=state["job_id"], identity=state["identity"]
            )
            is None
        )

    async def existing(_db):
        return state

    monkeypatch.setattr(historical, "ready_purge", existing)
    # Reuse the real provisioner completed-probe, signed native settlement,
    # repeated archive and public Delete regression over the issued old row.
    await historical.test_ready_purge_actual_completed_probe_allows_repeated_archive_and_public_delete(
        db, monkeypatch
    )
