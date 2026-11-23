"""The llm_endpoints TLS pin columns against real PostgreSQL.

Pins: the format and pair CHECKs, and that the three endpoint reads plus
resolve_catalog_model return the pin (NULL for an unpinned row).
"""

from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import asyncpg
import pytest
import pytest_asyncio
from testcontainers.postgres import PostgresContainer

from orchestrator.database.postgres import PostgresDB
from shared.tls_pinning import CertificateDetails

ROOT = Path(__file__).resolve().parents[1]
FINGERPRINT = ":".join(["AB"] * 32)
PIN_KEYS = (
    "tls_pin_sha256",
    "tls_pin_cert",
    "tls_pin_subject",
    "tls_pin_issuer",
    "tls_pin_not_after",
    "tls_pinned_at",
    "tls_pinned_by",
)


@pytest.fixture(scope="module")
def pg_dsn():
    with PostgresContainer("postgres:16-alpine") as postgres:
        yield postgres.get_connection_url().replace("postgresql+psycopg2", "postgresql")


@pytest_asyncio.fixture(scope="module")
async def schema_applied(pg_dsn):
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(
            (ROOT / "src/orchestrator/database/schema_current.sql").read_text()
        )
    finally:
        await conn.close()


@pytest_asyncio.fixture
async def db(pg_dsn, schema_applied):
    store = PostgresDB(connection_string=pg_dsn, min_connections=1, max_connections=4)
    await store.connect()
    try:
        await store.execute("TRUNCATE models, llm_endpoints CASCADE")
        yield store
    finally:
        await store.close()


async def _endpoint(db, label="Pinned"):
    row = await db.create_system_llm_endpoint(
        label=label, base_url="https://10.0.0.1:8090/v1", api_key=None, key_prefix=None
    )
    return str(row["id"])


async def _pin(db, endpoint_id, sha=FINGERPRINT, cert="PEM"):
    await db.execute(
        "UPDATE llm_endpoints SET tls_pin_sha256 = $2, tls_pin_cert = $3,"
        " tls_pin_subject = 'CN=x', tls_pin_issuer = 'CN=x',"
        " tls_pin_not_after = now(), tls_pinned_at = now(),"
        " tls_pinned_by = $4::uuid WHERE id = $1::uuid",
        endpoint_id,
        sha,
        cert,
        str(uuid4()),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "bad", [":".join(["ab"] * 32), ":".join(["AB"] * 31), "AB" * 32]
)
async def test_format_check_rejects_malformed_fingerprint(db, bad):
    endpoint_id = await _endpoint(db)
    with pytest.raises(asyncpg.CheckViolationError):
        await _pin(db, endpoint_id, sha=bad)


@pytest.mark.asyncio
async def test_pair_check_rejects_fingerprint_without_certificate(db):
    endpoint_id = await _endpoint(db)
    with pytest.raises(asyncpg.CheckViolationError):
        await _pin(db, endpoint_id, cert=None)


@pytest.mark.asyncio
async def test_pair_check_rejects_certificate_without_fingerprint(db):
    endpoint_id = await _endpoint(db)
    with pytest.raises(asyncpg.CheckViolationError):
        await _pin(db, endpoint_id, sha=None)


@pytest.mark.asyncio
async def test_reads_return_null_pin_for_unpinned_row(db):
    endpoint_id = await _endpoint(db)
    for row in (
        await db.get_system_llm_endpoint(endpoint_id),
        (await db.list_system_llm_endpoints())[0],
    ):
        for key in PIN_KEYS:
            assert key in row and row[key] is None


@pytest.mark.asyncio
async def test_reads_return_the_pin(db):
    endpoint_id = await _endpoint(db)
    await _pin(db, endpoint_id)
    rows = [
        await db.get_system_llm_endpoint(endpoint_id),
        await db.get_user_llm_endpoint(endpoint_id),
        (await db.list_system_llm_endpoints())[0],
    ]
    for row in rows:
        assert row["tls_pin_sha256"] == FINGERPRINT
        assert row["tls_pin_cert"] == "PEM"
        assert row["tls_pin_subject"] == "CN=x"
        assert row["tls_pin_issuer"] == "CN=x"
        assert row["tls_pin_not_after"] is not None
        assert row["tls_pinned_at"] is not None
        assert row["tls_pinned_by"] is not None


@pytest.mark.asyncio
async def test_resolve_catalog_model_returns_the_pin(db):
    endpoint_id = await _endpoint(db)
    await db.create_model(
        provider_kind="endpoint",
        provider_ref=endpoint_id,
        model_id="pin-model",
        display_label="pin-model",
        capabilities=["chat"],
        family="default",
    )
    resolved = await db.resolve_catalog_model("pin-model")
    assert resolved["endpoint_tls_pin_sha256"] is None
    assert resolved["endpoint_tls_pin_cert"] is None
    await _pin(db, endpoint_id)
    resolved = await db.resolve_catalog_model("pin-model")
    assert resolved["endpoint_tls_pin_sha256"] == FINGERPRINT
    assert resolved["endpoint_tls_pin_cert"] == "PEM"


# ---------------------------------------------------------------------------
# set / clear (Task 5)
# ---------------------------------------------------------------------------

SECOND_FP = ":".join(["CD"] * 32)
ADMIN = str(uuid4())


def _details(
    sha=FINGERPRINT, pem="-----BEGIN CERTIFICATE-----\nX\n-----END CERTIFICATE-----\n"
):
    return CertificateDetails(
        sha256=sha,
        subject="CN=gpu-box",
        issuer="CN=Private CA",
        not_before=datetime(2025, 1, 1, tzinfo=timezone.utc),
        not_after=datetime(2030, 1, 2, 3, 4, 5, tzinfo=timezone.utc),
        expired=False,
        san=[],
        version=1,
        pem=pem,
    )


async def _events(db, endpoint_id):
    return await db.fetch(
        "SELECT * FROM security_events WHERE resource_id = $1 ORDER BY created_at",
        endpoint_id,
    )


async def _trust(db, endpoint_id, details=None):
    return await db.set_llm_endpoint_tls_pin(
        endpoint_id,
        details=details or _details(),
        admin_user_id=ADMIN,
        method="PUT",
        path=f"/api/admin/providers/endpoints/{endpoint_id}/tls-pin",
    )


@pytest.mark.asyncio
async def test_trust_writes_pin_and_one_trusted_event(db):
    endpoint_id = await _endpoint(db)
    row = await _trust(db, endpoint_id)
    assert row["tls_pin_sha256"] == FINGERPRINT
    assert row["tls_pin_cert"].startswith("-----BEGIN CERTIFICATE-----")
    assert row["tls_pin_subject"] == "CN=gpu-box"
    assert row["tls_pin_issuer"] == "CN=Private CA"
    assert row["tls_pin_not_after"] == datetime(
        2030, 1, 2, 3, 4, 5, tzinfo=timezone.utc
    )
    assert row["tls_pinned_at"] is not None
    assert str(row["tls_pinned_by"]) == ADMIN
    events = await _events(db, endpoint_id)
    assert len(events) == 1
    ev = events[0]
    assert ev["event_type"] == "endpoint_certificate_trusted"
    assert ev["resource_type"] == "llm_endpoint"
    assert ev["real_is_admin"] is True and ev["view_as"] is False
    assert str(ev["user_id"]) == ADMIN
    assert ev["method"] == "PUT"
    assert f"sha256={FINGERPRINT}" in ev["detail"]
    assert "label=Pinned" in ev["detail"]
    assert "origin=https://10.0.0.1:8090" in ev["detail"]
    assert "subject=CN=gpu-box" in ev["detail"]
    assert "not_after=2030-01-02T03:04:05+00:00" in ev["detail"]
    assert "previous=" not in ev["detail"]


@pytest.mark.asyncio
async def test_second_trust_is_retrusted_with_previous(db):
    endpoint_id = await _endpoint(db)
    await _trust(db, endpoint_id)
    row = await _trust(db, endpoint_id, _details(sha=SECOND_FP))
    assert row["tls_pin_sha256"] == SECOND_FP
    events = await _events(db, endpoint_id)
    assert [e["event_type"] for e in events] == [
        "endpoint_certificate_trusted",
        "endpoint_certificate_retrusted",
    ]
    assert f"previous={FINGERPRINT}" in events[1]["detail"]
    assert f"sha256={SECOND_FP}" in events[1]["detail"]


@pytest.mark.asyncio
async def test_trust_accepts_no_admin_id(db):
    endpoint_id = await _endpoint(db)
    row = await db.set_llm_endpoint_tls_pin(
        endpoint_id, details=_details(), admin_user_id=None, method=None, path=None
    )
    assert row["tls_pinned_by"] is None
    assert len(await _events(db, endpoint_id)) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("admin_id", ["admin", "not-a-uuid", 42])
async def test_a_malformed_admin_id_is_recorded_as_null(db, admin_id):
    # Like record_security_event: a non-UUID admin id must not turn a Trust or
    # Remove into a 500; the pin and its audit row are written without it.
    endpoint_id = await _endpoint(db)
    row = await db.set_llm_endpoint_tls_pin(
        endpoint_id, details=_details(), admin_user_id=admin_id, method="PUT", path="/x"
    )
    assert row["tls_pin_sha256"] == FINGERPRINT
    assert row["tls_pinned_by"] is None
    cleared = await db.clear_llm_endpoint_tls_pin(
        endpoint_id, admin_user_id=admin_id, method="DELETE", path="/x"
    )
    assert cleared is not None and cleared["tls_pin_sha256"] is None
    events = await _events(db, endpoint_id)
    assert [e["event_type"] for e in events] == [
        "endpoint_certificate_trusted",
        "endpoint_certificate_pin_removed",
    ]
    assert all(e["user_id"] is None for e in events)


@pytest.mark.asyncio
async def test_trust_unknown_or_user_scoped_row_writes_nothing(db):
    assert await _trust(db, str(uuid4())) is None
    user_id = str(uuid4())
    await db.execute(
        "INSERT INTO users (id, display_name, email) VALUES ($1::uuid, 'owner', $2)",
        user_id,
        f"{user_id}@example.test",
    )
    scoped = await db.fetchval(
        "INSERT INTO llm_endpoints (user_id, label, base_url)"
        " VALUES ($1::uuid, 'mine', 'https://u/v1') RETURNING id::text",
        user_id,
    )
    assert await _trust(db, scoped) is None
    assert await _events(db, scoped) == []
    assert (
        await db.fetchval(
            "SELECT tls_pin_sha256 FROM llm_endpoints WHERE id = $1::uuid", scoped
        )
    ) is None


@pytest.mark.asyncio
async def test_clear_removes_pin_and_audits(db):
    endpoint_id = await _endpoint(db)
    await _trust(db, endpoint_id)
    row = await db.clear_llm_endpoint_tls_pin(
        endpoint_id, admin_user_id=ADMIN, method="DELETE", path="/x"
    )
    assert row is not None
    for key in PIN_KEYS:
        assert row[key] is None
    events = await _events(db, endpoint_id)
    assert events[-1]["event_type"] == "endpoint_certificate_pin_removed"
    assert f"sha256={FINGERPRINT}" in events[-1]["detail"]
    assert events[-1]["real_is_admin"] is True


@pytest.mark.asyncio
async def test_clear_on_unpinned_row_returns_none_and_writes_nothing(db):
    endpoint_id = await _endpoint(db)
    assert (
        await db.clear_llm_endpoint_tls_pin(
            endpoint_id, admin_user_id=ADMIN, method="DELETE", path="/x"
        )
        is None
    )
    assert await _events(db, endpoint_id) == []
    assert (
        await db.clear_llm_endpoint_tls_pin(
            str(uuid4()), admin_user_id=ADMIN, method="DELETE", path="/x"
        )
        is None
    )


@pytest.mark.asyncio
async def test_failed_audit_insert_rolls_back_the_pin(db, monkeypatch):
    endpoint_id = await _endpoint(db)
    await _trust(db, endpoint_id)
    original = asyncpg.Connection.fetchrow

    async def broken(self, query, *args, **kw):
        if "INSERT INTO security_events" in query:
            raise RuntimeError("audit down")
        return await original(self, query, *args, **kw)

    real_execute = asyncpg.Connection.execute

    async def broken_execute(self, query, *args, **kw):
        if "INSERT INTO security_events" in query:
            raise RuntimeError("audit down")
        return await real_execute(self, query, *args, **kw)

    monkeypatch.setattr(asyncpg.Connection, "fetchrow", broken)
    monkeypatch.setattr(asyncpg.Connection, "execute", broken_execute)
    with pytest.raises(RuntimeError):
        await _trust(db, endpoint_id, _details(sha=SECOND_FP))
    with pytest.raises(RuntimeError):
        await db.clear_llm_endpoint_tls_pin(
            endpoint_id, admin_user_id=ADMIN, method="DELETE", path="/x"
        )
    monkeypatch.undo()
    row = await db.get_system_llm_endpoint(endpoint_id)
    assert row["tls_pin_sha256"] == FINGERPRINT
    assert len(await _events(db, endpoint_id)) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("new_url", ["https://h:443/v1/", "https://h:8443/v1"])
async def test_base_url_edit_keeps_the_pin(db, new_url):
    endpoint_id = str(
        (
            await db.create_system_llm_endpoint(
                label=f"Edit {new_url}",
                base_url="https://h/v1",
                api_key=None,
                key_prefix=None,
            )
        )["id"]
    )
    await _trust(db, endpoint_id)
    before = await db.get_system_llm_endpoint(endpoint_id)
    events_before = len(await _events(db, endpoint_id))
    returned = await db.update_system_llm_endpoint(
        endpoint_id, base_url=new_url, source="ui"
    )
    after = await db.get_system_llm_endpoint(endpoint_id)
    assert after["base_url"] == new_url
    for key in PIN_KEYS:
        assert after[key] == before[key] and after[key] is not None
        # The PATCH response is serialized from the returned row: it must show
        # the pin the edit kept, not ``tls_pin: null``.
        assert returned[key] == before[key], key
    assert len(await _events(db, endpoint_id)) == events_before


@pytest.mark.asyncio
async def test_noop_edit_returns_the_pin(db):
    endpoint_id = await _endpoint(db)
    await _trust(db, endpoint_id)
    before = await db.get_system_llm_endpoint(endpoint_id)
    returned = await db.update_system_llm_endpoint(endpoint_id)
    for key in PIN_KEYS:
        assert returned[key] == before[key] and returned[key] is not None, key


@pytest.mark.asyncio
@pytest.mark.parametrize("new_url", ["https://h/v1", "https://other-host/v1"])
async def test_helm_reconcile_keeps_the_pin(db, new_url):
    from orchestrator.seed.llm_config import SeedReport, _seed_endpoints

    endpoint_id = str(
        (
            await db.create_system_llm_endpoint(
                label="Helm EP", base_url="https://h/v1", api_key=None, key_prefix=None
            )
        )["id"]
    )
    await _trust(db, endpoint_id)
    before = await db.get_system_llm_endpoint(endpoint_id)
    events_before = len(await _events(db, endpoint_id))
    await _seed_endpoints(
        db,
        [{"label": "Helm EP", "baseUrl": new_url, "reconcile": True, "models": []}],
        SeedReport(),
    )
    after = await db.get_system_llm_endpoint(endpoint_id)
    assert after["base_url"] == new_url
    for key in PIN_KEYS:
        assert after[key] == before[key] and after[key] is not None
    assert len(await _events(db, endpoint_id)) == events_before
