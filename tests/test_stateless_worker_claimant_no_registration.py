"""Regression: pooled sandbox claimant with no agents registration.

Valid pooled sandbox claimant, no ``agents`` row, recovery disabled.
The stateless executor never registers (``run_stateless_server`` starts
without orchestrator registration or heartbeat), so the bundle must not
require a pinned-agent registration row.

This is the route-level regression required by
``stateless_worker_bundle_requires_pinned_registration.md`` §1. It exercises
the real service/route with explicit queue authority and asserts the absence
of the invented registration.
"""

from unittest.mock import AsyncMock

import pytest

from orchestrator.services import (
    job_start_bundle,
    job_workspace_authority,
)
from orchestrator.application import sessions as sessions_composition
from orchestrator.schemas import job_runtime as job_runtime_module
from orchestrator.security import access as access_module
from orchestrator.services import container_provisioner as container_provisioner_module
from orchestrator.services import (
    vm_workspace_recovery_store as vm_workspace_recovery_store_module,
)
import functools

UNIT_ID = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
POD_NAME = "srw-agent-stateless-d9d86bd6f-pll7v"
POD_UID = "aaaaaaaa-1111-2222-3333-bbbbbbbbbbbb"
WORKSPACE_GENERATION = "11111111-1111-4111-8111-111111111111"
WORKSPACE_RUNTIME = "22222222-2222-4222-8222-222222222222"
WORKSPACE_FINGERPRINT = "SHA256:" + ("A" * 43)

LEASED_ROW = {
    "unit_kind": "worker_batch",
    "state": "leased",
    "lease_token": 7,
    "leased_by": POD_NAME,
    "input_seq": 41,
    "consumed_seq": 12,
    "thread_status": "created",
    "thread_lane": "stateless",
    "thread_metadata": {},
}


class _AsyncCM:
    def __init__(self, value=None):
        self._value = value

    async def __aenter__(self):
        return self._value

    async def __aexit__(self, *exc):
        return False


class NoRegistrationFakeDB:
    """SQL-aware fake: explicit results per query, no generic True.

    Unknown queries fail the fixture instead of silently inventing a
    registration. Lease validity and authorization writes are separated so
    one generic True cannot satisfy all three.
    """

    def __init__(self, *, run_queue_row, job):
        self._row = run_queue_row
        self._job = job
        self.conn = AsyncMock()
        self.conn.transaction = lambda: _AsyncCM()
        self.agents_lookups = 0
        self.authorized = False

        async def _fetchrow(sql, *args):
            if "FROM run_queue AS queue LEFT JOIN threads" in sql:
                return self._row
            if "SELECT state, lease_token, leased_by FROM run_queue" in sql:
                return self._row
            if (
                "SELECT accepted_result AS receipt FROM vm_workspace_recovery_requests"
                in sql
            ):
                return None
            if "SELECT * FROM worker_batch_attempts" in sql:
                return None
            raise AssertionError(f"unexpected fetchrow SQL: {sql!r}")

        async def _fetchval(sql, *args):
            if "SELECT EXISTS (SELECT 1 FROM agents WHERE hostname" in sql:
                # Live stateless contract: pooled executors never register.
                self.agents_lookups += 1
                return False
            if "SELECT EXISTS (SELECT 1 FROM run_queue" in sql:
                # Final exact-token recheck: the live lease is still ours.
                assert args == (UNIT_ID, 7), args
                return True
            if "SELECT 1 FROM run_queue WHERE unit_id" in sql:
                # record_bundle_authorized queue lock: lease still current.
                return 1
            if "UPDATE worker_batch_attempts SET bundle_authorized_at" in sql:
                self.authorized = True
                return 1
            raise AssertionError(f"unexpected fetchval SQL: {sql!r}")

        self.conn.fetchrow = AsyncMock(side_effect=_fetchrow)
        self.conn.fetchval = AsyncMock(side_effect=_fetchval)
        self.managed_repository_authorities_are_current = AsyncMock(return_value=True)

    def acquire(self):
        return _AsyncCM(self.conn)

    async def get_job(self, jid):
        if str(jid) == UNIT_ID:
            return dict(self._job)
        return None

    async def get_thread(self, tid):
        return None

    def thread_datasource_lock(self, tid):
        return _AsyncCM()


def _worker_job():
    return {
        "id": UNIT_ID,
        "execution_lane": "stateless",
        "config_override": {"workspace": {"backend": "sandbox"}},
        "context": {
            "_workspace_contract": {
                "version": 1,
                "requested_backend": "sandbox",
                "assigned_backend": "sandbox",
                "assignment_source": "test",
            },
            "workspace_container": {
                "status": "ready",
                "provisioner": "k8s",
                "pod_ip": "10.0.0.8",
                "_runtime_incarnation": WORKSPACE_RUNTIME,
            },
        },
    }


def _sandbox_app(monkeypatch, *, repo_credentials=None):
    import dataclasses

    from fastapi import FastAPI

    from orchestrator import main as orch_main
    from orchestrator.routers.unit_claim import router

    monkeypatch.setenv("VM_WORKSPACE_RECOVERY_ENABLED", "false")
    # Server-owned pool identity the attestation must enforce (not request
    # parameters). The fake attestor below checks these exact values.
    monkeypatch.setenv("WORKSPACE_NAMESPACE", "superhuman-remote-worker")
    monkeypatch.setenv("AGENT_LABEL_NAME", "superhuman-remote-worker")
    monkeypatch.setenv("AGENT_LABEL_INSTANCE", "srw")
    db = NoRegistrationFakeDB(run_queue_row=dict(LEASED_ROW), job=_worker_job())
    store = vm_workspace_recovery_store_module.VMWorkspaceRecoveryStore(db)
    attest = AsyncMock(return_value=None)
    original_factory = functools.partial(
        sessions_composition.unit_claim_bundle_dependencies,
        orch_main.app.state.resources,
    )

    def _factory():
        deps = original_factory()
        return dataclasses.replace(deps, attest_stateless_claimant=attest)

    monkeypatch.setattr(orch_main.app.state.resources, "postgres_db", db)
    monkeypatch.setattr(access_module, "require_internal", AsyncMock())
    monkeypatch.setattr(
        vm_workspace_recovery_store_module,
        "VMWorkspaceRecoveryStore",
        lambda _db: store,
    )
    monkeypatch.setattr(
        sessions_composition, "unit_claim_bundle_dependencies", _factory
    )
    monkeypatch.setattr(
        job_workspace_authority,
        "resolve_subjob_inherited_workspace",
        AsyncMock(return_value=("proceed", None)),
    )
    if repo_credentials is None:
        job_start_request = job_runtime_module.JobStartRequest(
            job_id=UNIT_ID, description="work"
        )
    else:
        job_start_request = job_runtime_module.JobStartRequest(
            job_id=UNIT_ID,
            description="secret-bearing",
            managed_repository_credentials=repo_credentials,
        )
    monkeypatch.setattr(
        job_start_bundle,
        "build_job_start_request",
        AsyncMock(return_value=job_start_request),
    )
    attested = container_provisioner_module.WorkspaceRuntimeAttestation(
        backing_id="k8s-pod:superhuman-remote-worker:" + WORKSPACE_GENERATION,
        workspace_generation=WORKSPACE_GENERATION,
        runtime_incarnation=WORKSPACE_RUNTIME,
        ssh_host_key_fingerprint=WORKSPACE_FINGERPRINT,
        host="10.0.0.9",
        pod_ip="10.0.0.9",
        port=30022,
    )
    monkeypatch.setattr(
        container_provisioner_module.container_provisioner,
        "attest_workspace_runtime",
        AsyncMock(return_value=attested),
    )
    app = FastAPI()
    app.include_router(router)
    app.state.unit_claim_bundle_dependencies_factory = _factory
    return app, db, attest


@pytest.mark.asyncio
async def test_pooled_sandbox_claimant_without_registration_succeeds(monkeypatch):
    """Valid pooled sandbox claimant, no agents row, recovery disabled."""
    import httpx

    app, db, attest = _sandbox_app(monkeypatch)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get(
            f"/internal/units/{UNIT_ID}/claim-bundle",
            params={"lease_token": 7, "pod_name": POD_NAME, "pod_uid": POD_UID},
        )
    # The live stateless contract has no agents row; the bundle must still
    # authorize and record authorization evidence even with recovery disabled.
    # The incompatible registration lookup must be gone; claimant Pod
    # attestation (initial + final) replaces it.
    assert db.agents_lookups == 0
    assert attest.await_count == 2
    assert attest.await_args_list[0].args == (POD_NAME, POD_UID)
    assert attest.await_args_list[1].args == (POD_NAME, POD_UID)
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["unit_kind"] == "worker_batch"
    assert body["job"]["job_id"] == UNIT_ID
    assert db.authorized is True


@pytest.mark.asyncio
@pytest.mark.parametrize("recovery_enabled", ["false", "true"])
async def test_valid_sandbox_claimant_both_flag_settings(monkeypatch, recovery_enabled):
    """Authorization accounting stays unconditional across the flag."""
    import httpx

    app, db, attest = _sandbox_app(monkeypatch)
    monkeypatch.setenv("VM_WORKSPACE_RECOVERY_ENABLED", recovery_enabled)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get(
            f"/internal/units/{UNIT_ID}/claim-bundle",
            params={"lease_token": 7, "pod_name": POD_NAME, "pod_uid": POD_UID},
        )
    assert response.status_code == 200, response.text
    assert db.authorized is True
    assert attest.await_count == 2


@pytest.mark.asyncio
async def test_claimant_attestation_refusal_is_generic_403(monkeypatch):
    import httpx
    from fastapi import HTTPException

    app, db, attest = _sandbox_app(monkeypatch)
    attest.side_effect = HTTPException(403, "Lease validation failed")
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get(
            f"/internal/units/{UNIT_ID}/claim-bundle",
            params={"lease_token": 7, "pod_name": POD_NAME, "pod_uid": POD_UID},
        )
    assert response.status_code == 403
    assert response.json() == {"detail": "Lease validation failed"}
    assert db.authorized is False
    assert attest.await_count == 1


@pytest.mark.asyncio
async def test_unknown_claimant_authority_is_bounded_503(monkeypatch):
    """K8s unavailable is unknown authority: retryable, never a recovery event."""
    import httpx
    from fastapi import HTTPException

    app, db, attest = _sandbox_app(monkeypatch)
    attest.side_effect = HTTPException(503, "Claimant authority unavailable")
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get(
            f"/internal/units/{UNIT_ID}/claim-bundle",
            params={"lease_token": 7, "pod_name": POD_NAME, "pod_uid": POD_UID},
        )
    # Distinct transient status for the executor's bounded retry path
    # (ClaimBundleError 503 → driver-error release, same as 403). It must not
    # be disguised as a workspace-recovery 409.
    assert response.status_code == 503
    assert response.json() == {"detail": "Claimant authority unavailable"}
    assert "recovery" not in response.json()
    assert db.authorized is False


@pytest.mark.asyncio
async def test_final_lease_steal_after_attestation_refused(monkeypatch):
    """A lease stolen during slow assembly never authorizes stale authority."""
    import httpx

    app, db, attest = _sandbox_app(monkeypatch)

    async def _stolen(sql, *args):
        if "SELECT EXISTS (SELECT 1 FROM run_queue" in sql:
            return False
        if "SELECT 1 FROM run_queue WHERE unit_id" in sql:
            return None
        if "UPDATE worker_batch_attempts SET bundle_authorized_at" in sql:
            raise AssertionError("stolen lease must not authorize")
        raise AssertionError(f"unexpected fetchval SQL: {sql!r}")

    db.conn.fetchval.side_effect = _stolen
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get(
            f"/internal/units/{UNIT_ID}/claim-bundle",
            params={"lease_token": 7, "pod_name": POD_NAME, "pod_uid": POD_UID},
        )
    assert response.status_code == 403
    assert db.authorized is False
    # Initial attestation succeeded; the exact-token recheck after the final
    # claimant observation and slow assembly refused before any authorization
    # write.
    assert attest.await_count == 2


@pytest.mark.asyncio
async def test_claimant_replacement_after_initial_check_refused(monkeypatch):
    """A Pod replaced between observations never receives the bundle."""
    import httpx
    from fastapi import HTTPException

    app, db, attest = _sandbox_app(monkeypatch)
    attest.side_effect = [
        None,
        HTTPException(403, "Lease validation failed"),
    ]
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get(
            f"/internal/units/{UNIT_ID}/claim-bundle",
            params={"lease_token": 7, "pod_name": POD_NAME, "pod_uid": POD_UID},
        )
    assert response.status_code == 403
    assert db.authorized is False
    assert attest.await_count == 2


@pytest.mark.asyncio
async def test_malformed_uid_is_generic_403_without_attestation(monkeypatch):
    import httpx

    app, db, attest = _sandbox_app(monkeypatch)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get(
            f"/internal/units/{UNIT_ID}/claim-bundle",
            params={"lease_token": 7, "pod_name": POD_NAME, "pod_uid": "not-a-uid"},
        )
    assert response.status_code == 403
    assert db.authorized is False
    assert attest.await_count == 0


@pytest.mark.asyncio
async def test_stale_token_refused_before_attestation(monkeypatch):
    import httpx

    app, db, attest = _sandbox_app(monkeypatch)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get(
            f"/internal/units/{UNIT_ID}/claim-bundle",
            params={"lease_token": 6, "pod_name": POD_NAME, "pod_uid": POD_UID},
        )
    assert response.status_code == 403
    assert db.authorized is False
    assert attest.await_count == 0


@pytest.mark.asyncio
async def test_repo_revoked_during_final_claimant_lookup_refused(monkeypatch):
    """A repository revocation during the slow final lookup must refuse.

    The second claimant observation pauses; revocation lands mid-wait. The
    stale bundle must not issue credentials nor record authorization.
    """
    import asyncio

    import httpx

    app, db, attest = _sandbox_app(monkeypatch)
    credentials = [
        {
            "authority_id": "11111111-1111-4111-8111-111111111111",
            "generation": 3,
            "repo_name": "job-stateless",
            "access_mode": "write",
            "private_key": "hidden-runtime-bearer",
        }
    ]
    monkeypatch.setattr(
        job_start_bundle,
        "build_job_start_request",
        AsyncMock(
            return_value=job_runtime_module.JobStartRequest(
                job_id=UNIT_ID,
                description="secret-bearing",
                managed_repository_credentials=credentials,
            )
        ),
    )
    repo_current = True

    async def _repo_authority(current_credentials):
        assert current_credentials == credentials
        return repo_current

    db.managed_repository_authorities_are_current = AsyncMock(
        side_effect=_repo_authority
    )
    entered = asyncio.Event()
    release = asyncio.Event()
    calls = 0

    async def _pausing_attest(pod_name, pod_uid):
        nonlocal calls
        calls += 1
        if calls == 2:
            entered.set()
            await release.wait()
        return None

    attest.side_effect = _pausing_attest
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        task = asyncio.create_task(
            client.get(
                f"/internal/units/{UNIT_ID}/claim-bundle",
                params={"lease_token": 7, "pod_name": POD_NAME, "pod_uid": POD_UID},
            )
        )
        await asyncio.wait_for(entered.wait(), timeout=5)
        repo_current = False
        release.set()
        response = await task
    assert response.status_code == 409, response.text
    assert (
        response.json()["detail"]
        == "Job repository authority changed during bundle assembly"
    )
    assert "job" not in response.json()
    assert db.authorized is False


@pytest.mark.asyncio
async def test_workspace_identity_changed_during_final_claimant_lookup_refused(
    monkeypatch,
):
    """A workspace incarnation change during the slow final lookup must refuse."""
    import asyncio

    import httpx

    app, db, attest = _sandbox_app(monkeypatch)
    entered = asyncio.Event()
    release = asyncio.Event()
    calls = 0

    async def _pausing_attest(pod_name, pod_uid):
        nonlocal calls
        calls += 1
        if calls == 2:
            entered.set()
            await release.wait()
        return None

    attest.side_effect = _pausing_attest
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        task = asyncio.create_task(
            client.get(
                f"/internal/units/{UNIT_ID}/claim-bundle",
                params={"lease_token": 7, "pod_name": POD_NAME, "pod_uid": POD_UID},
            )
        )
        await asyncio.wait_for(entered.wait(), timeout=5)
        db._job["context"]["workspace_container"]["_runtime_incarnation"] = (
            "33333333-3333-4333-8333-333333333333"
        )
        release.set()
        response = await task
    assert response.status_code == 409, response.text
    assert (
        response.json()["detail"]
        == "Job workspace contract changed during bundle assembly"
    )
    assert "job" not in response.json()
    assert db.authorized is False


@pytest.mark.asyncio
async def test_lease_rotated_during_final_claimant_lookup_rejected(monkeypatch):
    """A lease theft/token rotation during the slow final lookup must reject."""
    import asyncio

    import httpx

    app, db, attest = _sandbox_app(monkeypatch)

    async def _dynamic_fetchval(sql, *args):
        if "SELECT EXISTS (SELECT 1 FROM run_queue" in sql:
            return db._row["lease_token"] == 7 and db._row["leased_by"] == POD_NAME
        if "SELECT 1 FROM run_queue WHERE unit_id" in sql:
            if db._row["lease_token"] == 7 and db._row["leased_by"] == POD_NAME:
                return 1
            return None
        if "UPDATE worker_batch_attempts SET bundle_authorized_at" in sql:
            db.authorized = True
            return 1
        raise AssertionError(f"unexpected fetchval SQL: {sql!r}")

    db.conn.fetchval.side_effect = _dynamic_fetchval
    entered = asyncio.Event()
    release = asyncio.Event()
    calls = 0

    async def _pausing_attest(pod_name, pod_uid):
        nonlocal calls
        calls += 1
        if calls == 2:
            entered.set()
            await release.wait()
        return None

    attest.side_effect = _pausing_attest
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        task = asyncio.create_task(
            client.get(
                f"/internal/units/{UNIT_ID}/claim-bundle",
                params={"lease_token": 7, "pod_name": POD_NAME, "pod_uid": POD_UID},
            )
        )
        await asyncio.wait_for(entered.wait(), timeout=5)
        db._row = dict(
            db._row, lease_token=8, leased_by="srw-agent-stateless-deadbeef-0000"
        )
        release.set()
        response = await task
    assert response.status_code == 403, response.text
    assert response.json() == {"detail": "Lease validation failed"}
    assert "job" not in response.json()
    assert db.authorized is False


@pytest.mark.asyncio
async def test_transport_auth_refused_before_attestation(monkeypatch):
    import httpx
    from unittest.mock import AsyncMock as _AsyncMock

    from fastapi import HTTPException as _HTTPException

    app, db, attest = _sandbox_app(monkeypatch)

    monkeypatch.setattr(
        access_module,
        "require_internal",
        _AsyncMock(side_effect=_HTTPException(401, "Unauthorized")),
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get(
            f"/internal/units/{UNIT_ID}/claim-bundle",
            params={"lease_token": 7, "pod_name": POD_NAME, "pod_uid": POD_UID},
        )
    assert response.status_code == 401
    assert attest.await_count == 0
    assert db.authorized is False
