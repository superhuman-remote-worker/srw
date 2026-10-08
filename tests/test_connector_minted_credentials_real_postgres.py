"""Real-PostgreSQL proofs for provider-minted credentials (connector drivers C5).

Minting, re-delivery, renewal past half a credential's life, every revoke
point (C2's, which request these revokes too, a connector change, and the
sweep's backstops), a revoke racing a mint, retries and giving up, the
lease exchange minting a GitHub App connector's upstream token, and the
delivery of a kubeconfig and of a GitHub App token, all against
``schema_current.sql`` and the fake provider APIs of
``tests/_provider_fakes.py``.
"""

from __future__ import annotations

import asyncio
import copy
import json
import logging
from pathlib import Path
from uuid import UUID, uuid4

import asyncpg
import pytest
import pytest_asyncio
import yaml
from testcontainers.postgres import PostgresContainer

from orchestrator.database.postgres import PostgresDB
from orchestrator.security.crypto import encrypt
from orchestrator.services import connector_credential_leases as leases
from orchestrator.services import connector_minted_credentials as minted
from orchestrator.services.connector_bind_time import (
    BindTimePending,
    BindTimeRefused,
)
from orchestrator.services.connector_driver_identities import mint_driver_identity
from orchestrator.services.connector_drivers import builtin_connector_drivers
from orchestrator.services.connector_drivers.base import BindContext, DeploymentGates
from orchestrator.services.connector_drivers.credential_files import KubeconfigDriver
from orchestrator.services.connector_lease_exchange import ConnectorLeaseExchange
from shared.connectors.builtin import GIT_SWAP_SPEC
from tests._provider_fakes import (
    FakeGitHubApi,
    FakeKubeApi,
    ProviderRouter,
    install,
    minting_kubeconfig_yaml,
    rsa_key_pair,
)

SCHEMA = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "orchestrator"
    / "database"
    / "schema_current.sql"
)
TOKEN_REQUEST = {
    "namespace": "srw-identities",
    "service_account": "agent",
    "expiration_seconds": 600,
}
APP = {"app_id": "4242", "installation_id": "9090"}
URL = "https://github.com/acme/repo.git"
PRIVATE, PUBLIC = rsa_key_pair()


@pytest.fixture(scope="module")
def pg_dsn():
    try:
        container = PostgresContainer("postgres:15")
        container.start()
    except Exception as exc:
        pytest.skip(f"local PostgreSQL container unavailable: {exc}")
    try:
        yield container.get_connection_url().replace(
            "postgresql+psycopg2", "postgresql"
        )
    finally:
        container.stop()


@pytest_asyncio.fixture(scope="module")
async def _schema_applied(pg_dsn):
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(SCHEMA.read_text())
    finally:
        await conn.close()


@pytest_asyncio.fixture
async def db(pg_dsn, _schema_applied):
    store = PostgresDB(connection_string=pg_dsn, min_connections=1, max_connections=8)
    await store.connect()
    async with store.acquire() as conn:
        await conn.execute(
            "TRUNCATE connector_minted_credentials, connector_credential_leases, "
            "connector_driver_identities, security_events, project_datasources, "
            "datasources, jobs, threads, projects CASCADE"
        )
    minted.configure_minted_credentials(minted.MintedRuntime(store=store))
    try:
        yield store
    finally:
        minted.configure_minted_credentials(None)
        await store.close()


@pytest.fixture
def router(monkeypatch):
    return install(
        monkeypatch,
        ProviderRouter(kube=FakeKubeApi(), github=FakeGitHubApi(public_key_pem=PUBLIC)),
    )


@pytest.fixture
def kube(router):
    return router.kube


@pytest.fixture
def github(router):
    return router.github


# =============================================================================
# Rows
# =============================================================================


async def _kube_connector(db, api: FakeKubeApi, *, token: str | None = None) -> str:
    connector_id = uuid4()
    credentials = {
        "files": [
            {
                "name": "cluster.yaml",
                "contents": minting_kubeconfig_yaml(token or api.minting_token),
                "target_path": "/home/srw/.kube/configs/cluster.yaml",
                "mode": "0600",
            }
        ]
    }
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO datasources (id, name, type, scope_mode, policy_revision, "
            "credentials, config) VALUES ($1, $2, 'kubeconfig', 'all', 1, "
            "$3::jsonb, $4::jsonb)",
            connector_id,
            f"cluster-{str(connector_id)[:8]}",
            json.dumps(encrypt(json.dumps(credentials))),
            json.dumps({"token_request": TOKEN_REQUEST}),
        )
    return str(connector_id)


async def _github_connector(db) -> str:
    connector_id = uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO datasources (id, name, type, scope_mode, policy_revision, "
            "connection_url, credentials, config) VALUES ($1, $2, 'repository', "
            "'all', 1, $3, $4::jsonb, $5::jsonb)",
            connector_id,
            f"repo-{str(connector_id)[:8]}",
            URL,
            json.dumps(
                encrypt(
                    json.dumps({"auth_method": "github_app", "private_key": PRIVATE})
                )
            ),
            json.dumps({"forge": "github", "github_app": APP}),
        )
    return str(connector_id)


async def _job(db, status="processing") -> str:
    job_id = uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO jobs (id, description, status) VALUES ($1, 'c5', $2)",
            job_id,
            status,
        )
    return str(job_id)


async def _thread(db, status="active", *, metadata=None, project=None) -> str:
    thread_id = uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO threads (id, status, metadata, project_id) "
            "VALUES ($1, $2, $3::jsonb, $4)",
            thread_id,
            status,
            json.dumps(metadata or {}),
            UUID(project) if project else None,
        )
    return str(thread_id)


async def _rows(db, connector=None):
    async with db.acquire() as conn:
        return await conn.fetch(
            "SELECT * FROM connector_minted_credentials "
            + ("WHERE connector_id = $1 " if connector else "")
            + "ORDER BY created_at",
            *([UUID(connector)] if connector else []),
        )


async def _events(db, event_type):
    async with db.acquire() as conn:
        return await conn.fetch(
            "SELECT * FROM security_events WHERE event_type = $1", event_type
        )


def _kube_entry(connector: str) -> dict:
    row = {
        "id": connector,
        "type": "kubeconfig",
        "name": "cluster",
        "config": {"token_request": TOKEN_REQUEST},
    }
    credentials = {
        "files": [
            {
                "name": "cluster.yaml",
                "contents": "never delivered",
                "target_path": "/home/srw/.kube/configs/cluster.yaml",
                "mode": "0600",
            }
        ]
    }
    ctx = BindContext(
        gates=DeploymentGates(lambda: True, lambda: True),
        logger=logging.getLogger("test"),
        default_known_hosts="",
    )
    return KubeconfigDriver().bind(row, credentials, ctx=ctx)


def _github_entry(connector: str, *, block=None, read_only=False) -> dict:
    entry = {
        "type": "repository",
        "name": "repo",
        "datasource_id": connector,
        "connection_url": URL,
        "config": {"forge": "github", "github_app": APP},
        "credentials": {"auth_method": "github_app"},
        "project_read_only": read_only,
        "minted": {"provider": "github_app", "connector_id": connector},
    }
    if block is not None:
        entry["git_swap"] = block
    return entry


async def _deliver(db, entries, owner):
    async with db.acquire() as conn:
        async with conn.transaction():
            return await minted.deliver_minted_entries(conn, entries, owner=owner)


def _delivered_token(entry) -> str:
    doc = yaml.safe_load(entry["credentials"]["files"][0]["contents"])
    return doc["users"][0]["user"]["token"]


async def _age(db, credential_id, *, left_seconds: int, lived_seconds: int):
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE connector_minted_credentials SET "
            "minted_at = now() - make_interval(secs => $2::int), "
            "expires_at = now() + make_interval(secs => $3::int) WHERE id = $1",
            credential_id,
            lived_seconds,
            left_seconds,
        )


# =============================================================================
# Kubernetes: mint, re-deliver, renew
# =============================================================================


@pytest.mark.asyncio
async def test_a_delivery_hands_out_a_kubeconfig_with_the_minted_token_only(db, kube):
    connector = await _kube_connector(db, kube)
    owner = leases.LeaseOwner.thread(await _thread(db))
    entry = _kube_entry(connector)
    await minted.prepare_minted_entries(db, [entry], owner=owner)
    assert len(kube.secrets) == 1

    first = copy.deepcopy(entry)
    assert await _deliver(db, [first], owner) == 1
    assert "minted" not in first
    token = _delivered_token(first)
    assert kube.authenticates(token)
    contents = first["credentials"]["files"][0]["contents"]
    assert kube.minting_token not in contents and "exec" not in contents
    assert first["credentials"]["files"][0]["target_path"] == (
        "/home/srw/.kube/configs/cluster.yaml"
    )
    # Every later delivery (a claim, an attach, a recycle) gets the same one.
    again = copy.deepcopy(entry)
    await _deliver(db, [again], owner)
    assert _delivered_token(again) == token
    assert len(kube.secrets) == 1
    [row] = await _rows(db)
    assert row["status"] == "live" and row["provider"] == "kubernetes"
    assert token not in str(dict(row)) and kube.minting_token not in str(dict(row))
    assert len(await _events(db, "connector_minted_credential_issued")) == 1


@pytest.mark.asyncio
async def test_past_half_its_life_a_delivery_mints_again_and_the_old_one_lapses(
    db, kube
):
    connector = await _kube_connector(db, kube)
    owner = leases.LeaseOwner.job(await _job(db))
    entry = _kube_entry(connector)
    first = copy.deepcopy(entry)
    await _deliver(db, [first], owner)  # no preparation: minted inline
    old = _delivered_token(first)
    [row] = await _rows(db)
    await _age(db, row["id"], left_seconds=200, lived_seconds=400)

    renewed = copy.deepcopy(entry)
    await _deliver(db, [renewed], owner)
    new = _delivered_token(renewed)
    assert new != old
    rows = await _rows(db)
    assert [r["status"] for r in rows] == ["superseded", "live"]
    # The old token still works until its own expiry: another work item on
    # the workspace may hold it.
    assert kube.authenticates(old) and kube.authenticates(new)
    # At its expiry the sweep deletes its Secret.
    await _age(db, row["id"], left_seconds=-1, lived_seconds=601)
    report = await minted.sweep_minted_once(db)
    assert report.expired == 1 and report.revoked == 1
    assert not kube.authenticates(old) and kube.authenticates(new)
    statuses = {str(r["id"]): r["status"] for r in await _rows(db)}
    assert statuses[str(row["id"])] == "revoked"
    # An expiry is housekeeping, not a revoke event.
    assert await _events(db, "connector_minted_credential_revoked") == []


# =============================================================================
# Revoke points
# =============================================================================


@pytest.mark.asyncio
async def test_end_revokes_through_the_lease_revoke_point(db, kube):
    connector = await _kube_connector(db, kube)
    thread = await _thread(db)
    owner = leases.LeaseOwner.thread(thread)
    entry = copy.deepcopy(_kube_entry(connector))
    await _deliver(db, [entry], owner)
    token = _delivered_token(entry)

    async with db.acquire() as conn:
        async with conn.transaction():
            await leases.revoke_execution_leases(
                conn, thread_id=thread, reason="session_end"
            )
    [row] = await _rows(db)
    assert row["status"] == "revoking" and row["revoke_reason"] == "session_end"
    assert kube.authenticates(token)  # the provider call is the sweep's

    report = await minted.sweep_minted_once(db)
    assert report.revoked == 1
    assert not kube.authenticates(token) and kube.secrets == {}
    [event] = await _events(db, "connector_minted_credential_revoked")
    assert "reason=session_end" in event["detail"]


@pytest.mark.asyncio
async def test_a_live_detach_revokes_that_connector_only_and_a_fallback_none(db, kube):
    first, second = await _kube_connector(db, kube), await _kube_connector(db, kube)
    owner = leases.LeaseOwner.thread(await _thread(db))
    await _deliver(db, [copy.deepcopy(_kube_entry(c)) for c in (first, second)], owner)
    async with db.acquire() as conn:
        await leases.revoke_connector_leases(
            conn, owner=owner, connector_ids=[second], reason="served_by_fallback"
        )
        assert {r["status"] for r in await _rows(db)} == {"live"}
        await leases.revoke_connector_leases(conn, owner=owner, connector_ids=[second])
    assert (await _rows(db, first))[0]["status"] == "live"
    assert (await _rows(db, second))[0]["status"] == "revoking"


@pytest.mark.asyncio
async def test_a_connector_delete_revokes_and_the_record_outlives_the_row(db, kube):
    connector = await _kube_connector(db, kube)
    owner = leases.LeaseOwner.job(await _job(db))
    entry = copy.deepcopy(_kube_entry(connector))
    await _deliver(db, [entry], owner)
    async with db.acquire() as conn:
        async with conn.transaction():
            await leases.revoke_all_connector_leases(conn, connector_id=connector)
            await conn.execute("DELETE FROM datasources WHERE id = $1", UUID(connector))
    await minted.sweep_minted_once(db)
    assert kube.secrets == {}
    assert not kube.authenticates(_delivered_token(entry))


@pytest.mark.asyncio
async def test_a_connector_change_revokes_what_was_minted(db, kube):
    connector = await _kube_connector(db, kube)
    owner = leases.LeaseOwner.thread(await _thread(db))
    await _deliver(db, [copy.deepcopy(_kube_entry(connector))], owner)
    async with db.acquire() as conn:
        assert await minted.connector_changed(conn, connector) == 1
    [row] = await _rows(db)
    assert row["revoke_reason"] == "connector_changed"


@pytest.mark.asyncio
@pytest.mark.parametrize("ending", ["completed", "deleted"])
async def test_the_sweep_revokes_for_an_execution_that_ended_or_is_gone(
    db, kube, ending
):
    connector = await _kube_connector(db, kube)
    job = await _job(db)
    entry = copy.deepcopy(_kube_entry(connector))
    await _deliver(db, [entry], leases.LeaseOwner.job(job))
    async with db.acquire() as conn:
        if ending == "deleted":
            await conn.execute("DELETE FROM jobs WHERE id = $1", UUID(job))
        else:
            await conn.execute(
                "UPDATE jobs SET status = 'completed' WHERE id = $1", UUID(job)
            )
    report = await minted.sweep_minted_once(db)
    assert report.ended == 1 and report.revoked == 1
    assert not kube.authenticates(_delivered_token(entry))


@pytest.mark.asyncio
async def test_a_paused_job_keeps_its_token_until_it_expires(db, kube):
    connector = await _kube_connector(db, kube)
    job = await _job(db)
    entry = copy.deepcopy(_kube_entry(connector))
    await _deliver(db, [entry], leases.LeaseOwner.job(job))
    async with db.acquire() as conn:
        await conn.execute("UPDATE jobs SET status = 'paused' WHERE id = $1", UUID(job))
    assert (await minted.sweep_minted_once(db)).revoked == 0
    [row] = await _rows(db)
    await _age(db, row["id"], left_seconds=-1, lived_seconds=601)
    assert (await minted.sweep_minted_once(db)).expired == 1
    assert kube.secrets == {}


@pytest.mark.asyncio
async def test_an_end_during_a_mint_revokes_what_the_mint_made(db, kube, monkeypatch):
    connector = await _kube_connector(db, kube)
    thread = await _thread(db)
    owner = leases.LeaseOwner.thread(thread)
    real_mint = minted._mint

    async def ended_meanwhile(*args, **kwargs):
        result = await real_mint(*args, **kwargs)
        async with db.acquire() as conn:
            await leases.revoke_execution_leases(
                conn, thread_id=thread, reason="session_end"
            )
            await conn.execute(
                "UPDATE threads SET status = 'ended' WHERE id = $1", UUID(thread)
            )
        return result

    monkeypatch.setattr(minted, "_mint", ended_meanwhile)
    with pytest.raises(minted.MintFailure):
        await minted.ensure_minted(
            db, owner=owner, connector_id=connector, access="ReadWrite"
        )
    [row] = await _rows(db)
    assert row["status"] == "revoking" and row["token_ciphertext"] is not None
    assert len(kube.secrets) == 1
    await minted.sweep_minted_once(db)
    assert kube.secrets == {}


@pytest.mark.asyncio
async def test_a_terminal_execution_gets_nothing_minted(db, kube):
    connector = await _kube_connector(db, kube)
    job = await _job(db, status="cancelled")
    with pytest.raises(minted.MintFailure, match="no longer accepts"):
        await minted.ensure_minted(
            db,
            owner=leases.LeaseOwner.job(job),
            connector_id=connector,
            access="ReadWrite",
        )
    assert kube.requests == [] and await _rows(db) == []


@pytest.mark.asyncio
async def test_two_deliveries_minting_at_once_share_one_credential(db, kube):
    connector = await _kube_connector(db, kube)
    owner = leases.LeaseOwner.thread(await _thread(db))
    first, second = await asyncio.gather(
        *(
            minted.ensure_minted(
                db, owner=owner, connector_id=connector, access="ReadWrite"
            )
            for _ in range(2)
        )
    )
    assert first.token == second.token
    rows = await _rows(db)
    assert sorted(r["status"] for r in rows) in (["live"], ["live", "revoking"])
    if len(rows) == 2:
        [raced] = [r for r in rows if r["status"] == "revoking"]
        assert raced["revoke_reason"] == "mint_raced"
    await minted.sweep_minted_once(db)
    assert len(kube.secrets) == 1 and kube.authenticates(first.token)


@pytest.mark.asyncio
async def test_a_mint_the_provider_refuses_leaves_nothing_behind(db, kube):
    connector = await _kube_connector(db, kube)
    owner = leases.LeaseOwner.thread(await _thread(db))
    kube.forbidden.add("token")
    with pytest.raises(minted.MintFailure) as caught:
        await minted.ensure_minted(
            db, owner=owner, connector_id=connector, access="ReadWrite"
        )
    assert caught.value.permanent
    [row] = await _rows(db)
    assert row["status"] == "revoking" and row["revoke_reason"] == "mint_failed"
    assert kube.secrets == {}


@pytest.mark.asyncio
async def test_a_failed_revoke_backs_off_then_gives_up_audited(db, kube):
    connector = await _kube_connector(db, kube)
    thread = await _thread(db)
    await _deliver(
        db, [copy.deepcopy(_kube_entry(connector))], leases.LeaseOwner.thread(thread)
    )
    kube.forbidden.add("delete-secret")
    async with db.acquire() as conn:
        await leases.revoke_execution_leases(
            conn, thread_id=thread, reason="session_end"
        )
    report = await minted.sweep_minted_once(db)
    assert report.retried == 1
    [row] = await _rows(db)
    assert row["revoke_attempts"] == 1 and "HTTP 403" in row["revoke_error"]
    assert row["revoke_next_at"] > row["revoke_requested_at"]
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE connector_minted_credentials SET revoke_next_at = now(), "
            "revoke_attempts = $2 WHERE id = $1",
            row["id"],
            minted.MAX_REVOKE_ATTEMPTS - 1,
        )
    report = await minted.sweep_minted_once(db)
    assert report.given_up == 1
    [row] = await _rows(db)
    assert row["status"] == "revoked" and row["revoke_error"]
    assert len(await _events(db, "connector_minted_revoke_abandoned")) == 1


@pytest.mark.asyncio
async def test_an_abandoned_mint_is_revoked_by_name(db, kube):
    connector = await _kube_connector(db, kube)
    job = await _job(db)
    credential_id = uuid4()
    material = {
        "server": "https://kube.test:6443",
        "ca": None,
        "token": kube.minting_token,
        "namespace": "srw-identities",
        "secret": {"name": f"srw-mint-{credential_id.hex}", "uid": None},
    }
    kube.secrets[f"srw-mint-{credential_id.hex}"] = {"uid": "left-behind"}
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO connector_minted_credentials (id, owner_kind, owner_id, "
            "connector_id, provider, access, config_digest, material_ciphertext, "
            "created_at) VALUES ($1, 'job', $2, $3, 'kubernetes', 'ReadWrite', $4, "
            "$5, now() - interval '1 hour')",
            credential_id,
            UUID(job),
            UUID(connector),
            "sha256:" + "0" * 64,
            encrypt(json.dumps(material)),
        )
    report = await minted.sweep_minted_once(db)
    assert report.abandoned == 1 and report.revoked == 1
    assert kube.secrets == {}


@pytest.mark.asyncio
async def test_revoked_rows_are_pruned_after_the_retention(db, kube):
    connector = await _kube_connector(db, kube)
    thread = await _thread(db)
    await _deliver(
        db, [copy.deepcopy(_kube_entry(connector))], leases.LeaseOwner.thread(thread)
    )
    async with db.acquire() as conn:
        await leases.revoke_execution_leases(
            conn, thread_id=thread, reason="session_end"
        )
    await minted.sweep_minted_once(db)
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE connector_minted_credentials SET revoked_at = now() - interval '31 days'"
        )
    assert (await minted.sweep_minted_once(db)).pruned == 1
    assert await _rows(db) == []


@pytest.mark.asyncio
async def test_constraints_reject_malformed_rows(db):
    base = (
        "INSERT INTO connector_minted_credentials (id, owner_kind, owner_id, "
        "connector_id, provider, access, config_digest, material_ciphertext, "
        "status, token_ciphertext, expires_at, minted_at) VALUES ($1, $2, $3, $3, "
        "$4, 'ReadOnly', $5, 'x', $6, $7, $8, $8)"
    )
    digest = "sha256:" + "0" * 64
    bad = [
        ("session", "kubernetes", digest, "minting", None, None),
        ("job", "aws", digest, "minting", None, None),
        ("job", "kubernetes", "md5:0", "minting", None, None),
        ("job", "kubernetes", digest, "live", None, None),  # live without token
        ("job", "kubernetes", digest, "revoking", None, None),  # no request
    ]
    async with db.acquire() as conn:
        for kind, provider, config_digest, status, token, at in bad:
            with pytest.raises(asyncpg.CheckViolationError):
                await conn.execute(
                    base,
                    uuid4(),
                    kind,
                    uuid4(),
                    provider,
                    config_digest,
                    status,
                    token,
                    at,
                )


# =============================================================================
# Delivery failures
# =============================================================================


@pytest.mark.asyncio
async def test_a_session_skips_a_connector_it_cannot_mint_for_with_a_notice(db, kube):
    connector = await _kube_connector(db, kube)
    kube.forbidden.add("token")
    entry = copy.deepcopy(_kube_entry(connector))
    owner = leases.LeaseOwner.thread(await _thread(db))
    assert await _deliver(db, [entry], owner) == 0
    assert entry["credentials"] == {}
    assert (
        entry["cli_hint"].startswith("Not delivered: ") and "403" in entry["cli_hint"]
    )


@pytest.mark.asyncio
async def test_a_job_waits_for_a_provider_that_did_not_answer_and_fails_on_a_refusal(
    db, kube
):
    connector = await _kube_connector(db, kube)
    owner = leases.LeaseOwner.job(await _job(db))
    kube.fail["create-secret"] = [503]
    with pytest.raises(BindTimePending):
        await _deliver(db, [copy.deepcopy(_kube_entry(connector))], owner)
    kube.forbidden.add("create-secret")
    with pytest.raises(BindTimeRefused, match="403"):
        await _deliver(db, [copy.deepcopy(_kube_entry(connector))], owner)


# =============================================================================
# GitHub App
# =============================================================================


@pytest.mark.asyncio
async def test_the_exchange_mints_a_github_app_token_for_the_lease(db, github):
    connector = await _github_connector(db)
    thread = await _thread(db)
    owner = leases.LeaseOwner.thread(thread)
    async with db.acquire() as conn:
        lease = await leases.issue_or_redeliver(
            conn,
            owner=owner,
            connector_id=connector,
            driver=GIT_SWAP_SPEC.name,
            access="ReadOnly",
        )
        identity = await mint_driver_identity(
            conn, connector_id=connector, driver=GIT_SWAP_SPEC.name, pod_uid="p"
        )
    exchange = ConnectorLeaseExchange(
        store=db, drivers=builtin_connector_drivers(git_swap_image="img")
    )
    first = await exchange.exchange(
        identity_token=identity.token, lease_token=lease.token, operation="read"
    )
    assert first.status == 200, first.body
    token = first.body["credential"]
    assert token.startswith("ghs_")
    assert github.live(token)["permissions"] == {
        "contents": "read",
        "metadata": "read",
    }
    assert github.live(token)["repositories"] == ["repo"]
    assert first.body["allowed_upstream"] == ["https://github.com/acme/repo.git"]
    second = await exchange.exchange(
        identity_token=identity.token, lease_token=lease.token, operation="read"
    )
    assert second.body["credential"] == token
    assert len(github.tokens) == 1

    # The lease's level moves up: a write token, and the read one is revoked.
    async with db.acquire() as conn:
        await leases.issue_or_redeliver(
            conn,
            owner=owner,
            connector_id=connector,
            driver=GIT_SWAP_SPEC.name,
            access="ReadWrite",
        )
    third = await exchange.exchange(
        identity_token=identity.token, lease_token=lease.token, operation="write"
    )
    assert third.status == 200 and third.body["credential"] != token
    assert github.live(third.body["credential"])["permissions"]["contents"] == "write"
    await minted.sweep_minted_once(db)
    assert github.live(token) is None

    # End: the lease and the token go.
    async with db.acquire() as conn:
        await leases.revoke_execution_leases(
            conn, thread_id=thread, reason="session_end"
        )
    await minted.sweep_minted_once(db)
    assert github.live(third.body["credential"]) is None


@pytest.mark.asyncio
async def test_a_github_app_the_exchange_cannot_mint_for_answers_a_retry(db, github):
    github.repositories = {"elsewhere"}
    connector = await _github_connector(db)
    owner = leases.LeaseOwner.thread(await _thread(db))
    async with db.acquire() as conn:
        lease = await leases.issue_or_redeliver(
            conn,
            owner=owner,
            connector_id=connector,
            driver=GIT_SWAP_SPEC.name,
            access="ReadOnly",
        )
        identity = await mint_driver_identity(
            conn, connector_id=connector, driver=GIT_SWAP_SPEC.name, pod_uid="p"
        )
    exchange = ConnectorLeaseExchange(
        store=db, drivers=builtin_connector_drivers(git_swap_image="img")
    )
    answer = await exchange.exchange(
        identity_token=identity.token, lease_token=lease.token, operation="read"
    )
    assert answer.status == 503
    assert answer.body == {"error": "upstream_credential_refused"}


@pytest.mark.asyncio
async def test_the_fallback_delivers_the_minted_token_and_the_swap_nothing(db, github):
    connector = await _github_connector(db)
    owner = leases.LeaseOwner.job(await _job(db))
    fallback = _github_entry(connector, block={"fallback": "why"}, read_only=True)
    served = _github_entry(connector, block={"url": "https://driver/x"})
    refused = _github_entry(connector, block={"unavailable": "why"})
    assert await _deliver(db, [fallback, served, refused], owner) == 1
    assert fallback["credentials"]["auth_method"] == "token"
    token = fallback["credentials"]["token"]
    assert github.live(token)["permissions"]["contents"] == "read"
    assert served["credentials"] == {"auth_method": "github_app"}
    assert refused["credentials"] == {"auth_method": "github_app"}
    assert "minted" not in fallback and "minted" not in served


@pytest.mark.asyncio
async def test_a_sessions_stored_selection_is_minted_at_its_project_links_level(
    db, github
):
    connector = await _github_connector(db)
    project = uuid4()
    async with db.acquire() as conn:
        await conn.execute("INSERT INTO projects (id, name) VALUES ($1, 'c5')", project)
        await conn.execute(
            "INSERT INTO project_datasources (project_id, datasource_id, read_only) "
            "VALUES ($1, $2, true)",
            project,
            UUID(connector),
        )
    thread = await _thread(
        db, metadata={"datasource_ids": [connector]}, project=str(project)
    )
    await minted.prepare_thread_minted(db, thread)
    [row] = await _rows(db)
    assert row["access"] == "ReadOnly" and row["status"] == "live"
    [token] = github.tokens
    assert github.tokens[token]["permissions"]["contents"] == "read"
