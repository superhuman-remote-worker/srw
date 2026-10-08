"""Real-PostgreSQL proofs for provider-minted credentials (connector drivers C5).

Minting in a delivery's preparation (never in its transaction), re-delivery,
renewal past half a credential's life, every revoke point (C2's, which
request these revokes too, a connector edit through the API, and the
sweep's backstops), a revoke racing a mint, two mints racing each other,
retries until expiry and giving up, revoked rows that keep no secret, Test's
recorded mints, the lease exchange minting a GitHub App connector's
upstream token, the delivery of a kubeconfig and of a GitHub App token, and
the sweeper holding its LISTEN only while there is work, all against
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
    KUBE_SERVER,
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
async def db(pg_dsn, _schema_applied, router):
    # After ``router``: its install sets aside any runtime an earlier test
    # left, and this store is the one minting records go to.
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
        await minted.settle_background_mints()
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


async def _kube_connector(
    db, api: FakeKubeApi, *, token: str | None = None, server: str = KUBE_SERVER
) -> str:
    connector_id = uuid4()
    credentials = {
        "files": [
            {
                "name": "cluster.yaml",
                "contents": minting_kubeconfig_yaml(
                    token or api.minting_token, server=server
                ),
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


async def _github_connector(db, *, read_only: bool = False) -> str:
    connector_id = uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO datasources (id, name, type, scope_mode, policy_revision, "
            "connection_url, credentials, config, read_only) VALUES ($1, $2, "
            "'repository', 'all', 1, $3, $4::jsonb, $5::jsonb, $6)",
            connector_id,
            f"repo-{str(connector_id)[:8]}",
            URL,
            json.dumps(
                encrypt(
                    json.dumps({"auth_method": "github_app", "private_key": PRIVATE})
                )
            ),
            json.dumps({"forge": "github", "github_app": APP}),
            read_only,
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


async def _deliver_only(db, entries, owner):
    """The delivery inside its transaction, with no preparation (a live
    update)."""
    async with db.acquire() as conn:
        async with conn.transaction():
            return await minted.deliver_minted_entries(conn, entries, owner=owner)


async def _deliver(db, entries, owner):
    """A delivery as the claim, attach and dispatch paths run it: prepared
    before its transaction."""
    await minted.prepare_minted_entries(db, entries, owner=owner)
    return await _deliver_only(db, entries, owner)


def _material_of(row) -> dict:
    from orchestrator.security.crypto import decrypt

    return json.loads(decrypt(row["material_ciphertext"]))


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
    await _deliver(db, [first], owner)
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


@pytest.mark.asyncio
async def test_a_delivery_never_mints_and_carries_the_preparations_outcome(db, kube):
    connector = await _kube_connector(db, kube)
    entry = _kube_entry(connector)
    # Nothing prepared (a live update): a session's connector is skipped
    # with a notice, and minted in the background for a later delivery.
    session = leases.LeaseOwner.thread(await _thread(db))
    skipped = copy.deepcopy(entry)
    assert await _deliver_only(db, [skipped], session) == 0
    assert skipped["credentials"] == {}
    assert skipped["cli_hint"].startswith("Not delivered yet: the credential is")
    await minted.settle_background_mints()
    later = copy.deepcopy(entry)
    assert await _deliver_only(db, [later], session) == 1
    assert kube.authenticates(_delivered_token(later))
    # A job's delivery waits for it the same way.
    job = leases.LeaseOwner.job(await _job(db))
    with pytest.raises(BindTimePending, match="being minted"):
        await _deliver_only(db, [copy.deepcopy(entry)], job)
    await minted.settle_background_mints()
    assert await _deliver_only(db, [copy.deepcopy(entry)], job) == 1
    # A preparation's refusal reaches the delivery, which makes no provider
    # call of its own (and starts no background mint).
    refused_job = leases.LeaseOwner.job(await _job(db))
    kube.forbidden.add("token")
    await minted.prepare_minted_entries(db, [copy.deepcopy(entry)], owner=refused_job)
    calls = len(kube.requests)
    with pytest.raises(BindTimeRefused, match="HTTP 403"):
        await _deliver_only(db, [copy.deepcopy(entry)], refused_job)
    await minted.settle_background_mints()
    assert len(kube.requests) == calls


async def _hanging_kube(kube, monkeypatch, *, hang_host: str = "kube.test"):
    """The fake API server, except that ``hang_host`` accepts and never
    answers; returns the event that releases it."""
    import httpx

    from orchestrator.services.connector_drivers import provider_http

    release = asyncio.Event()
    router = ProviderRouter(kube=kube)

    async def handler(request):
        if (request.headers.get("host") or "").split(":")[0] == hang_host:
            await release.wait()
        return router(request)

    transport = httpx.MockTransport(handler)
    monkeypatch.setitem(
        provider_http._state,
        "factory",
        lambda *, verify=True, timeout=10.0: httpx.AsyncClient(transport=transport),
    )
    return release


@pytest.mark.asyncio
async def test_a_dispatch_never_waits_on_a_provider(db, kube, monkeypatch):
    import time

    connector = await _kube_connector(db, kube)
    owner = leases.LeaseOwner.job(await _job(db))
    entries = [copy.deepcopy(_kube_entry(connector))]
    release = await _hanging_kube(kube, monkeypatch)
    for _ in range(3):
        started = time.monotonic()
        # The pinned dispatch's and the job claim's preparation (bind_wait=0).
        await leases.prepare_lease_delivery(db, entries, owner=owner, bind_wait=0)
        with pytest.raises(BindTimePending, match="being minted|took too long"):
            await _deliver_only(db, copy.deepcopy(entries), owner)
        assert time.monotonic() - started < 1
    # One mint in flight for the job and connector, one record.
    assert len(minted._inflight) == 1 and len(await _rows(db)) == 1
    release.set()
    await minted.settle_background_mints()
    delivered = copy.deepcopy(entries)
    assert await _deliver_only(db, delivered, owner) == 1
    assert kube.authenticates(_delivered_token(delivered[0]))


@pytest.mark.asyncio
async def test_a_dead_providers_job_never_stalls_another_jobs_dispatch(
    db, kube, monkeypatch
):
    import time

    dead = await _kube_connector(db, kube)
    alive = await _kube_connector(db, kube, server="https://kube2.test:6443")
    stuck = leases.LeaseOwner.job(await _job(db))
    other = leases.LeaseOwner.job(await _job(db))
    release = await _hanging_kube(kube, monkeypatch)
    dispatch_lock = asyncio.Lock()  # the dispatcher's global lock

    async def dispatch(owner, connector) -> bool:
        entries = [copy.deepcopy(_kube_entry(connector))]
        async with dispatch_lock:
            await leases.prepare_lease_delivery(db, entries, owner=owner, bind_wait=0)
            try:
                await _deliver_only(db, entries, owner)
            except BindTimePending:
                return False
            return True

    started = time.monotonic()
    assert not await dispatch(stuck, dead)
    assert not await dispatch(other, alive)
    # The other job's mint finishes in the background; the next tick
    # dispatches it while the dead provider's mint still hangs.
    for _ in range(50):
        if any(r["status"] == "live" for r in await _rows(db, alive)):
            break
        await asyncio.sleep(0.05)
    assert not await dispatch(stuck, dead)
    assert await dispatch(other, alive)
    assert time.monotonic() - started < 5
    release.set()
    await minted.settle_background_mints()


@pytest.mark.asyncio
async def test_settling_background_mints_is_bounded(db, kube, monkeypatch):
    import time

    connector = await _kube_connector(db, kube)
    owner = leases.LeaseOwner.job(await _job(db))
    release = await _hanging_kube(kube, monkeypatch)
    await leases.prepare_lease_delivery(
        db, [copy.deepcopy(_kube_entry(connector))], owner=owner, bind_wait=0
    )
    started = time.monotonic()
    await minted.settle_background_mints(timeout=0.2)
    assert time.monotonic() - started < 1 and minted._inflight
    release.set()
    await minted.settle_background_mints()


@pytest.mark.asyncio
async def test_a_connector_change_forgets_the_remembered_failure(db, kube):
    connector = await _kube_connector(db, kube)
    owner = leases.LeaseOwner.job(await _job(db))
    kube.forbidden.add("token")
    await minted.prepare_minted_entries(
        db, [copy.deepcopy(_kube_entry(connector))], owner=owner
    )
    assert minted._remembered(owner, connector) is not None
    async with db.acquire() as conn:
        await minted.connector_changed(conn, connector)
    assert minted._remembered(owner, connector) is None


@pytest.mark.asyncio
async def test_past_half_its_life_an_unprepared_delivery_keeps_the_valid_one(db, kube):
    connector = await _kube_connector(db, kube)
    owner = leases.LeaseOwner.thread(await _thread(db))
    entry = _kube_entry(connector)
    first = copy.deepcopy(entry)
    await _deliver(db, [first], owner)
    old = _delivered_token(first)
    [row] = await _rows(db)
    await _age(db, row["id"], left_seconds=200, lived_seconds=400)
    # A live update past half the token's life: the still-valid token goes
    # out, and the renewal runs in the background.
    meanwhile = copy.deepcopy(entry)
    assert await _deliver_only(db, [meanwhile], owner) == 1
    assert _delivered_token(meanwhile) == old
    await minted.settle_background_mints()
    assert [r["status"] for r in await _rows(db)] == ["superseded", "live"]
    renewed = copy.deepcopy(entry)
    await _deliver_only(db, [renewed], owner)
    assert _delivered_token(renewed) != old


@pytest.mark.asyncio
async def test_a_slow_mint_is_not_cancelled_by_its_preparations_bound(
    db, kube, monkeypatch
):
    connector = await _kube_connector(db, kube)
    owner = leases.LeaseOwner.job(await _job(db))
    release = asyncio.Event()
    real_mint = minted._mint

    async def slow(*args, **kwargs):
        await release.wait()
        return await real_mint(*args, **kwargs)

    monkeypatch.setattr(minted, "_mint", slow)
    monkeypatch.setattr(minted, "PREPARE_MINT_SECONDS", 0.05)
    entry = _kube_entry(connector)
    await minted.prepare_minted_entries(db, [copy.deepcopy(entry)], owner=owner)
    with pytest.raises(BindTimePending, match="took too long"):
        await _deliver_only(db, [copy.deepcopy(entry)], owner)
    # The mint goes on and records what it made, for the next delivery.
    release.set()
    await minted.settle_background_mints()
    [row] = await _rows(db)
    assert row["status"] == "live"
    delivered = copy.deepcopy(entry)
    assert await _deliver_only(db, [delivered], owner) == 1
    assert kube.authenticates(_delivered_token(delivered))


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
    # A revoked row keeps what it named, never the token or the minting
    # credential.
    [row] = await _rows(db)
    assert row["status"] == "revoked" and row["token_ciphertext"] is None
    material = _material_of(row)
    assert "token" not in material and material["secret"]["name"].startswith(
        "srw-mint-"
    )
    assert kube.minting_token not in json.dumps(material)


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


async def _owner_user(db) -> dict:
    async with db.acquire() as conn:
        return dict(
            await conn.fetchrow(
                "INSERT INTO users(display_name,is_approved,is_admin) "
                "VALUES('c5-owner',TRUE,TRUE) RETURNING *"
            )
        )


async def _api_update(db, user, connector: str, **body) -> None:
    from types import SimpleNamespace
    from unittest.mock import AsyncMock, MagicMock

    from orchestrator.schemas.datasources import DatasourceUpdate
    from orchestrator.services import datasources as datasource_service

    await datasource_service.update_datasource(
        request=SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace())),
        datasource_id=connector,
        body=DatasourceUpdate(**body),
        user=user,
        existing_ds=await db.get_datasource(connector),
        require_project_owner=AsyncMock(),
        dependencies=datasource_service.DatasourceDependencies(
            store=db,
            vector_db=MagicMock(),
            knowledge_index=MagicMock(),
            mcp_datasources_enabled=lambda: True,
            mcp_stdio_enabled=lambda: True,
            validate_mcp_datasource=lambda _url, _creds: None,
            connector_drivers=builtin_connector_drivers(),
        ),
    )


@pytest.mark.asyncio
async def test_a_connector_edit_through_the_api_revokes_what_was_minted(db, kube):
    user = await _owner_user(db)
    created = await db.create_datasource(
        name="cluster",
        ds_type="kubeconfig",
        connection_url=None,
        credentials={
            "files": [
                {
                    "name": "cluster.yaml",
                    "contents": minting_kubeconfig_yaml(kube.minting_token),
                    "target_path": "/home/srw/.kube/configs/cluster.yaml",
                    "mode": "0600",
                }
            ]
        },
        config={"token_request": TOKEN_REQUEST},
        created_by=str(user["id"]),
    )
    connector = str(created["id"])
    owner = leases.LeaseOwner.thread(await _thread(db))
    await _deliver(db, [copy.deepcopy(_kube_entry(connector))], owner)
    # A name or description edit changes nothing SRW mints with.
    await _api_update(db, user, connector, description="the cluster")
    [row] = await _rows(db)
    assert row["status"] == "live"
    # A minting-input edit: revoked as the update returns, before anything
    # else the update does.
    await _api_update(
        db,
        user,
        connector,
        config={"token_request": {**TOKEN_REQUEST, "expiration_seconds": 900}},
    )
    [row] = await _rows(db)
    assert row["status"] == "revoking" and row["revoke_reason"] == "connector_changed"
    await minted.sweep_minted_once(db)
    assert kube.secrets == {}


def test_which_edits_change_the_minting_inputs():
    from orchestrator.services.connector_drivers.base import NormalizedConnector
    from orchestrator.services.datasources import minting_inputs_changed

    minting = {"type": "kubeconfig", "config": {"token_request": TOKEN_REQUEST}}
    static = {"type": "kubeconfig", "config": {}}
    nothing = NormalizedConnector(None, None, None)
    assert not minting_inputs_changed(minting, nothing)
    assert minting_inputs_changed(minting, NormalizedConnector(None, {}, None))
    assert minting_inputs_changed(
        minting, NormalizedConnector(None, None, {"files": []})
    )
    assert minting_inputs_changed(
        {
            "type": "repository",
            "config": {"github_app": APP},
            "connection_url": URL,
            "credentials": {"auth_method": "github_app"},
        },
        NormalizedConnector("https://github.com/acme/other.git", None, None),
    )
    assert not minting_inputs_changed(static, NormalizedConnector(None, {}, None))


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
async def test_two_deliveries_minting_at_once_share_one_credential(
    db, kube, monkeypatch
):
    connector = await _kube_connector(db, kube)
    owner = leases.LeaseOwner.thread(await _thread(db))
    # Both mint at the provider before either records: the records are
    # serialised, and the loser's token is revoked, never dropped.
    both_minted = asyncio.Barrier(2)
    made: list[str] = []
    real_mint = minted._mint

    async def in_step(*args, **kwargs):
        result = await real_mint(*args, **kwargs)
        made.append(result.token)
        await both_minted.wait()
        return result

    monkeypatch.setattr(minted, "_mint", in_step)
    first, second = await asyncio.gather(
        *(
            minted.ensure_minted(
                db, owner=owner, connector_id=connector, access="ReadWrite"
            )
            for _ in range(2)
        )
    )
    assert first.token == second.token
    [loser] = [token for token in made if token != first.token]
    rows = await _rows(db)
    assert sorted(r["status"] for r in rows) == ["live", "revoking"]
    [raced] = [r for r in rows if r["status"] == "revoking"]
    assert raced["revoke_reason"] == "mint_raced"
    assert raced["token_ciphertext"] is not None
    await minted.sweep_minted_once(db)
    assert len(kube.secrets) == 1 and kube.authenticates(first.token)
    assert not kube.authenticates(loser)


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
    # Its Secret was deleted again: nothing is left, so the record is done
    # (no revoke, no "may be left" warning), and keeps no secret.
    [row] = await _rows(db)
    assert row["status"] == "revoked" and row["revoke_reason"] == "mint_failed"
    assert "token" not in _material_of(row)
    assert kube.secrets == {}
    assert (await minted.sweep_minted_once(db)).any() is False


@pytest.mark.asyncio
@pytest.mark.parametrize("how", ["refused", "never-reached"])
async def test_a_secret_create_that_made_nothing_leaves_a_done_record(
    db, kube, monkeypatch, caplog, how
):
    connector = await _kube_connector(db, kube)
    owner = leases.LeaseOwner.thread(await _thread(db))
    if how == "refused":
        kube.forbidden.add("create-secret")
    else:
        from tests._provider_fakes import install

        # The API server's name resolves to a refused address.
        install(
            monkeypatch,
            ProviderRouter(kube=kube),
            addresses={"kube.test": ("169.254.169.254",)},
        )
        minted.configure_minted_credentials(minted.MintedRuntime(store=db))
    with caplog.at_level(logging.WARNING, logger=minted.__name__):
        with pytest.raises(minted.MintFailure):
            await minted.ensure_minted(
                db, owner=owner, connector_id=connector, access="ReadWrite"
            )
        await minted.sweep_minted_once(db)
    [row] = await _rows(db)
    assert row["status"] == "revoked" and row["revoke_reason"] == "mint_failed"
    assert "Giving up" not in caplog.text
    assert await _events(db, "connector_minted_revoke_abandoned") == []


@pytest.mark.asyncio
async def test_a_secret_create_whose_answer_was_lost_is_swept_by_name(
    db, kube, monkeypatch
):
    connector = await _kube_connector(db, kube)
    owner = leases.LeaseOwner.thread(await _thread(db))
    real = kube.handle

    def lost(request):
        answer = real(request)
        if request.method == "POST" and request.url.path.endswith("/secrets"):
            raise __import__("httpx").ReadError("connection reset", request=request)
        return answer

    kube.handle = lost
    with pytest.raises(minted.MintFailure):
        await minted.ensure_minted(
            db, owner=owner, connector_id=connector, access="ReadWrite"
        )
    [row] = await _rows(db)
    assert row["status"] == "revoking" and len(kube.secrets) == 1
    kube.handle = real
    assert (await minted.sweep_minted_once(db)).revoked == 1
    assert kube.secrets == {}


@pytest.mark.asyncio
async def test_a_failed_revoke_is_retried_while_the_token_lives_then_abandoned(
    db, kube, caplog
):
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
    # Past the attempts cap, but the token is still valid: SRW keeps trying.
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE connector_minted_credentials SET revoke_next_at = now(), "
            "revoke_attempts = $2 WHERE id = $1",
            row["id"],
            minted.MAX_REVOKE_ATTEMPTS + 3,
        )
    report = await minted.sweep_minted_once(db)
    assert report.retried == 1 and report.given_up == 0
    # Once it has expired, the bound Secret left behind is abandoned: a
    # state of its own, logged and audited.
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE connector_minted_credentials SET revoke_next_at = now(), "
            "expires_at = now() - interval '1 second' WHERE id = $1",
            row["id"],
        )
    with caplog.at_level(logging.WARNING, logger=minted.__name__):
        report = await minted.sweep_minted_once(db)
    assert report.given_up == 1
    [row] = await _rows(db)
    assert row["status"] == "abandoned" and row["revoked_at"] is not None
    assert row["token_ciphertext"] is None and "HTTP 403" in row["revoke_error"]
    assert "token" not in _material_of(row)
    assert "Giving up revoking minted credential" in caplog.text
    assert "left at the provider" in caplog.text
    [event] = await _events(db, "connector_minted_revoke_abandoned")
    assert "status=abandoned" in event["detail"]
    assert len(kube.secrets) == 1  # what is left, named by the row
    # Never picked up again.
    assert (await minted.sweep_minted_once(db)).any() is False


@pytest.mark.asyncio
async def test_each_revoke_is_bounded_and_a_few_run_at_once(db, kube, monkeypatch):
    import time

    import httpx

    from orchestrator.services.connector_drivers import provider_http

    owners = []
    # Three API server hosts, the first with two credentials.
    for server in (
        KUBE_SERVER,
        KUBE_SERVER,
        "https://kube2.test:6443",
        "https://kube3.test:6443",
    ):
        thread = await _thread(db)
        await _deliver(
            db,
            [
                copy.deepcopy(
                    _kube_entry(await _kube_connector(db, kube, server=server))
                )
            ],
            leases.LeaseOwner.thread(thread),
        )
        owners.append(thread)
    # The API server accepts the DELETEs and never answers.
    in_flight, most = [0], [0]
    router = ProviderRouter(kube=kube)

    async def handler(request):
        if request.method == "DELETE":
            in_flight[0] += 1
            most[0] = max(most[0], in_flight[0])
            try:
                await asyncio.sleep(3600)
            finally:
                in_flight[0] -= 1
        return router(request)

    transport = httpx.MockTransport(handler)
    monkeypatch.setitem(
        provider_http._state,
        "factory",
        lambda *, verify=True, timeout=10.0: httpx.AsyncClient(transport=transport),
    )
    monkeypatch.setattr(minted, "REVOKE_ROW_SECONDS", 0.3)
    async with db.acquire() as conn:
        for thread in owners:
            await leases.revoke_execution_leases(
                conn, thread_id=thread, reason="session_end"
            )
    started = time.monotonic()
    report = await minted.sweep_minted_once(db)
    assert time.monotonic() - started < 3
    # One per host at a time, the hosts at once: the dead first host's
    # second credential waits for the next pass instead of a second slot.
    assert most[0] == 3
    assert report.retried == 3 and report.revoked == 0
    rows = await _rows(db)
    assert all(r["status"] == "revoking" for r in rows)
    tried = [r for r in rows if r["revoke_attempts"] == 1]
    assert len(tried) == 3
    assert all(r["revoke_error"] == "the revoke took too long" for r in tried)
    assert sorted(r["provider_host"] for r in rows) == [
        "kube.test:6443",
        "kube.test:6443",
        "kube2.test:6443",
        "kube3.test:6443",
    ]


@pytest.mark.asyncio
async def test_one_dead_hosts_backlog_never_fills_the_sweep_window(
    db, kube, monkeypatch
):
    from orchestrator.services.connector_drivers import provider_http

    monkeypatch.setattr(minted, "REVOKES_PER_PASS", 5)
    dead = await _kube_connector(db, kube)
    healthy = await _kube_connector(db, kube, server="https://kube2.test:6443")
    owners = []
    for connector in [dead] * 8 + [healthy]:
        thread = await _thread(db)
        await _deliver(
            db,
            [copy.deepcopy(_kube_entry(connector))],
            leases.LeaseOwner.thread(thread),
        )
        owners.append(thread)
    # kube.test stops resolving; kube2.test still answers.
    from tests._provider_fakes import fake_resolver

    monkeypatch.setitem(
        provider_http._state,
        "network",
        provider_http.ProviderNetwork(
            resolver=fake_resolver({"kube2.test": ("203.0.113.11",)})
        ),
    )
    async with db.acquire() as conn:
        for thread in owners:
            await leases.revoke_execution_leases(
                conn, thread_id=thread, reason="session_end"
            )
    report = await minted.sweep_minted_once(db)
    # The healthy host's revoke runs in the first pass, beside one of the
    # dead host's; the dead host's others wait, untried.
    assert report.revoked == 1 and report.retried == 1
    [done] = [r for r in await _rows(db, healthy)]
    assert done["status"] == "revoked"
    attempts = sorted(r["revoke_attempts"] for r in await _rows(db, dead))
    assert attempts == [0] * 7 + [1]


@pytest.mark.asyncio
async def test_a_record_whose_ca_does_not_load_is_abandoned_at_once(db, kube):
    connector = await _kube_connector(db, kube)
    thread = await _thread(db)
    await _deliver(
        db, [copy.deepcopy(_kube_entry(connector))], leases.LeaseOwner.thread(thread)
    )
    async with db.acquire() as conn:
        await leases.revoke_execution_leases(
            conn, thread_id=thread, reason="session_end"
        )
    [row] = await _rows(db)
    material = _material_of(row)
    material["ca"] = "-----BEGIN CERTIFICATE-----\nnot one\n-----END CERTIFICATE-----\n"
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE connector_minted_credentials SET material_ciphertext = $2 "
            "WHERE id = $1",
            row["id"],
            encrypt(json.dumps(material)),
        )
    report = await minted.sweep_minted_once(db)
    assert report.given_up == 1 and report.retried == 0
    [row] = await _rows(db)
    assert row["status"] == "abandoned" and row["revoke_attempts"] == 1


@pytest.mark.asyncio
async def test_a_record_srw_cannot_read_is_abandoned_at_once(db, kube):
    connector = await _kube_connector(db, kube)
    thread = await _thread(db)
    await _deliver(
        db, [copy.deepcopy(_kube_entry(connector))], leases.LeaseOwner.thread(thread)
    )
    async with db.acquire() as conn:
        await leases.revoke_execution_leases(
            conn, thread_id=thread, reason="session_end"
        )
        await conn.execute(
            "UPDATE connector_minted_credentials SET material_ciphertext = 'garbled'"
        )
    report = await minted.sweep_minted_once(db)
    assert report.given_up == 1 and report.retried == 0
    [row] = await _rows(db)
    assert row["status"] == "abandoned" and row["revoke_attempts"] == 1
    assert "does not decrypt" in row["revoke_error"]
    assert row["token_ciphertext"] is None


@pytest.mark.asyncio
async def test_a_github_token_whose_revoke_failed_is_dead_at_its_expiry(db, github):
    connector = await _github_connector(db)
    thread = await _thread(db)
    owner = leases.LeaseOwner.thread(thread)
    entry = _github_entry(connector, block={"fallback": "why"})
    await _deliver(db, [entry], owner)
    token = entry["credentials"]["token"]
    async with db.acquire() as conn:
        await leases.revoke_execution_leases(
            conn, thread_id=thread, reason="session_end"
        )
    github.fail = [502]
    assert (await minted.sweep_minted_once(db)).retried == 1
    assert github.live(token)
    [row] = await _rows(db)
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE connector_minted_credentials SET revoke_next_at = now(), "
            "expires_at = now() - interval '1 second' WHERE id = $1",
            row["id"],
        )
    report = await minted.sweep_minted_once(db)
    assert report.revoked == 1
    [row] = await _rows(db)
    assert row["status"] == "revoked" and row["token_ciphertext"] is None


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
            "connector_id, provider, provider_host, access, config_digest, "
            "material_ciphertext, created_at) VALUES ($1, 'job', $2, $3, "
            "'kubernetes', 'kube.test:6443', 'ReadWrite', $4, $5, "
            "now() - interval '1 hour')",
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
        "connector_id, provider, provider_host, access, config_digest, "
        "material_ciphertext, status, token_ciphertext, expires_at, minted_at) "
        "VALUES ($1, $2, $3, $3, $4, 'h', 'ReadOnly', $5, 'x', $6, $7, $8, $8)"
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
    db, kube, monkeypatch
):
    connector = await _kube_connector(db, kube)
    owner = leases.LeaseOwner.job(await _job(db))
    kube.fail["create-secret"] = [503]
    with pytest.raises(BindTimePending, match="server error"):
        await _deliver(db, [copy.deepcopy(_kube_entry(connector))], owner)
    # A retry while the failure is fresh asks the provider nothing.
    calls = len(kube.requests)
    with pytest.raises(BindTimePending, match="server error"):
        await _deliver(db, [copy.deepcopy(_kube_entry(connector))], owner)
    assert len(kube.requests) == calls and len(await _rows(db)) == 1
    # Once stale, it is tried again.
    monkeypatch.setattr(minted, "TRANSIENT_OUTCOME_SECONDS", 0.0)
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
    # GitHub's documented user for an installation token.
    assert first.body["username"] == "x-access-token"
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
async def test_a_static_tokens_exchange_answer_keeps_the_forge_username(db):
    connector = uuid4()
    token = "glpat-" + "t" * 20
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO datasources (id, name, type, scope_mode, policy_revision, "
            "connection_url, credentials, config) VALUES ($1, 'gl', 'repository', "
            "'all', 1, $2, $3::jsonb, $4::jsonb)",
            connector,
            "https://gitlab.example/acme/repo.git",
            json.dumps(encrypt(json.dumps({"auth_method": "token", "token": token}))),
            json.dumps({"forge": "gitlab"}),
        )
        lease = await leases.issue_or_redeliver(
            conn,
            owner=leases.LeaseOwner.thread(await _thread(db)),
            connector_id=str(connector),
            driver=GIT_SWAP_SPEC.name,
            access="ReadOnly",
        )
        identity = await mint_driver_identity(
            conn, connector_id=str(connector), driver=GIT_SWAP_SPEC.name, pod_uid="p"
        )
    answer = await ConnectorLeaseExchange(
        store=db, drivers=builtin_connector_drivers(git_swap_image="img")
    ).exchange(identity_token=identity.token, lease_token=lease.token, operation="read")
    assert answer.status == 200, answer.body
    assert answer.body["credential"] == token
    assert answer.body["username"] == "oauth2"


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
    owner = leases.LeaseOwner.job(await _job(db))
    fallback = _github_entry(
        await _github_connector(db), block={"fallback": "why"}, read_only=True
    )
    served = _github_entry(
        await _github_connector(db), block={"url": "https://driver/x"}
    )
    refused = _github_entry(await _github_connector(db), block={"unavailable": "why"})
    assert await _deliver(db, [fallback, served, refused], owner) == 1
    assert fallback["credentials"]["auth_method"] == "token"
    token = fallback["credentials"]["token"]
    assert github.live(token)["permissions"]["contents"] == "read"
    assert fallback["credentials"]["username"] == "x-access-token"
    assert served["credentials"] == {"auth_method": "github_app"}
    assert refused["credentials"] == {"auth_method": "github_app"}
    assert "minted" not in fallback and "minted" not in served


@pytest.mark.asyncio
async def test_a_token_a_fallback_exposed_is_revoked_once_the_driver_serves(db, github):
    connector = await _github_connector(db)
    owner = leases.LeaseOwner.thread(await _thread(db))
    fallback = _github_entry(connector, block={"fallback": "why"})
    await _deliver(db, [fallback], owner)
    exposed = fallback["credentials"]["token"]
    [row] = await _rows(db)
    assert row["delivered_at"] is not None
    # The driver serves the connector now (installed, or reachable again):
    # the clone URL's token goes, and the exchange mints one the workspace
    # never saw.
    served = _github_entry(connector, block={"url": "https://driver/x"})
    await _deliver(db, [served], owner)
    statuses = {r["revoke_reason"]: r["status"] for r in await _rows(db)}
    assert statuses.get("exposed_before_swap") == "revoking"
    await minted.sweep_minted_once(db)
    assert github.live(exposed) is None
    # A token the swap alone used is never revoked for exposure.
    other = await _github_connector(db)
    await _deliver(db, [_github_entry(other, block={"url": "https://driver/x"})], owner)
    await _deliver(db, [_github_entry(other, block={"url": "https://driver/x"})], owner)
    assert all(
        r["revoke_reason"] != "exposed_before_swap" for r in await _rows(db, other)
    )


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


@pytest.mark.asyncio
async def test_a_read_only_connector_never_gets_a_write_token(db, github):
    """A public (or read-only) GitHub App connector is minted contents: read
    in preparation, delivery and the exchange alike, whatever the project
    link says."""
    connector = await _github_connector(db, read_only=True)
    row = await db.get_datasource(connector)
    entry = _github_entry(connector, block={"fallback": "why"})
    entry["minted"] = minted.github_app_marker(row)
    assert entry["minted"]["read_only"] is True
    owner = leases.LeaseOwner.job(await _job(db))
    assert await _deliver(db, [entry], owner) == 1
    token = entry["credentials"]["token"]
    assert github.live(token)["permissions"]["contents"] == "read"
    [record] = await _rows(db)
    assert record["access"] == "ReadOnly"
    # The exchange asks for ReadWrite (the lease's level): still read.
    exchange_owner = leases.LeaseOwner.thread(await _thread(db))
    upstream = await minted.minted_lease_upstream(
        db, row, owner=exchange_owner, access="ReadWrite"
    )
    assert github.live(upstream.token)["permissions"]["contents"] == "read"
    # A session's preparation reads the connector's own rule too.
    thread = await _thread(db, metadata={"datasource_ids": [connector]})
    await minted.prepare_thread_minted(db, thread)
    access = {r["owner_id"]: r["access"] for r in await _rows(db)}
    assert access[UUID(thread)] == "ReadOnly"


@pytest.mark.asyncio
async def test_an_overbroad_token_that_did_not_revoke_is_kept_for_the_sweep(db, github):
    import httpx

    connector = await _github_connector(db)
    owner = leases.LeaseOwner.job(await _job(db))
    github.grant_extra = {"administration": "write"}
    real = github.handle

    def no_revoke(request):
        if request.method == "DELETE":
            return httpx.Response(503, json={"message": "unavailable"})
        return real(request)

    github.handle = no_revoke
    with pytest.raises(minted.MintFailure):
        await minted.ensure_minted(
            db, owner=owner, connector_id=connector, access="ReadOnly"
        )
    [record] = await _rows(db)
    assert record["status"] == "revoking" and record["revoke_reason"] == "overbroad"
    assert record["token_ciphertext"] is not None
    [token] = github.tokens
    github.handle = real
    assert (await minted.sweep_minted_once(db)).revoked == 1
    assert github.live(token) is None


@pytest.mark.asyncio
async def test_a_legacy_multi_project_session_reads_its_strictest_link(db, github):
    connector = await _github_connector(db)
    writable, read_only, elsewhere = uuid4(), uuid4(), uuid4()
    async with db.acquire() as conn:
        for project, flag in ((writable, False), (read_only, True), (elsewhere, True)):
            await conn.execute(
                "INSERT INTO projects (id, name) VALUES ($1, $2)", project, str(project)
            )
            await conn.execute(
                "INSERT INTO project_datasources (project_id, datasource_id, "
                "read_only) VALUES ($1, $2, $3)",
                project,
                UUID(connector),
                flag,
            )
    # Two projects in scope (the legacy metadata list): one link is
    # read-only, so the session never gets a write token.
    legacy = await _thread(
        db,
        metadata={
            "datasource_ids": [connector],
            "project_ids": [str(writable), str(read_only)],
        },
    )
    await minted.prepare_thread_minted(db, legacy)
    [row] = await _rows(db)
    assert row["access"] == "ReadOnly"
    # A read-only link to a project outside the session's scope does not
    # count.
    scoped = await _thread(
        db, metadata={"datasource_ids": [connector]}, project=str(writable)
    )
    await minted.prepare_thread_minted(db, scoped)
    access = {r["owner_id"]: r["access"] for r in await _rows(db)}
    assert access[UUID(scoped)] == "ReadWrite"


# =============================================================================
# Test
# =============================================================================


@pytest.mark.asyncio
async def test_tests_mint_is_recorded_and_its_failed_revoke_is_swept(
    db, kube, monkeypatch
):
    monkeypatch.setattr(minted, "TEST_MINT_INTERVAL_SECONDS", 0.0)
    connector = await _kube_connector(db, kube)
    row = await db.get_datasource(connector)
    result = await KubeconfigDriver().check(row, row["credentials"], ctx=None)
    assert result["status"] == "ok", result
    [record] = await _rows(db)
    assert record["owner_kind"] == "test" and record["status"] == "revoked"
    assert record["revoke_reason"] == "test" and record["token_ciphertext"] is None
    assert kube.secrets == {}
    # A delete that fails is left to the sweep, which finishes it.
    kube.forbidden.add("delete-secret")
    result = await KubeconfigDriver().check(row, row["credentials"], ctx=None)
    assert result["status"] == "error" and "SRW keeps trying" in result["message"]
    [left] = [r for r in await _rows(db) if r["status"] == "revoking"]
    assert left["owner_kind"] == "test" and len(kube.secrets) == 1
    kube.forbidden.discard("delete-secret")
    report = await minted.sweep_minted_once(db)
    assert report.revoked == 1 and kube.secrets == {}
    # Test's revokes are no revoke event.
    assert await _events(db, "connector_minted_credential_revoked") == []


@pytest.mark.asyncio
async def test_one_user_tests_a_connector_at_most_every_few_seconds(db, kube):
    from types import SimpleNamespace

    connector = await _kube_connector(db, kube)
    row = await db.get_datasource(connector)
    alice, bob = SimpleNamespace(requester="alice"), SimpleNamespace(requester="bob")
    first = await KubeconfigDriver().check(row, row["credentials"], ctx=alice)
    assert first["status"] == "ok", first
    again = await KubeconfigDriver().check(row, row["credentials"], ctx=alice)
    assert again["status"] == "error" and "tested a moment ago" in again["message"]
    other = await KubeconfigDriver().check(row, row["credentials"], ctx=bob)
    assert other["status"] == "ok", other
    assert len(await _rows(db)) == 2


# =============================================================================
# The sweeper's LISTEN
# =============================================================================


@pytest.mark.asyncio
async def test_the_sweeper_listens_only_while_there_is_work(db, kube, monkeypatch):
    from orchestrator.services import connector_service_hosting

    assert await minted.pending(db) is False
    listening: list[str] = []

    async def fake_listen(store, wake, shutdown, *, channel):
        listening.append("start")
        try:
            await shutdown.wait()
        finally:
            listening.append("stop")

    monkeypatch.setattr(connector_service_hosting, "_listen", fake_listen)
    shutdown = asyncio.Event()
    sweeper = asyncio.create_task(
        minted.connector_minted_credential_sweeper(
            shutdown, store=db, interval_seconds=0.05
        )
    )

    async def until(predicate):
        for _ in range(200):
            if predicate():
                return
            await asyncio.sleep(0.02)
        raise AssertionError("timed out")

    try:
        await asyncio.sleep(0.2)
        assert listening == []  # idle: no connection held
        thread = await _thread(db)
        await _deliver(
            db,
            [copy.deepcopy(_kube_entry(await _kube_connector(db, kube)))],
            leases.LeaseOwner.thread(thread),
        )
        assert await minted.pending(db) is True
        await until(lambda: listening == ["start"])
        async with db.acquire() as conn:
            await leases.revoke_execution_leases(
                conn, thread_id=thread, reason="session_end"
            )
        await until(lambda: kube.secrets == {})
        await until(lambda: listening == ["start", "stop"])
        assert await minted.pending(db) is False
    finally:
        shutdown.set()
        await asyncio.wait_for(sweeper, 5)
