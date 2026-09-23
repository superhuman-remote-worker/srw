"""GET /internal/units/{unit_id}/claim-bundle (stateless_agents.md §5.6, M4).

The pinned executor-facing contract: internal-key transport auth PLUS live
lease proof ((unit_id, lease_token) must match state='leased' with the exact
token, one SELECT that also reads the watermarks), then the attach payload
assembled by the SAME `_assemble_session_attach_payload` the pinned-lane
sender uses. Error taxonomy: 401 bad internal key; 403 generic on any lease
mismatch (no enumeration oracle); 404 absent unit row; 409 non-session unit /
wrong lane / assembly refused.
"""

from orchestrator.services.workspace_lifecycle import WorkspaceOwner
import asyncio
from copy import deepcopy
from unittest.mock import ANY, AsyncMock, MagicMock
from uuid import UUID

import pytest
from fastapi import HTTPException

# R1.B06: the route moved to ``routers/unit_claim`` and its policy to
# ``services/unit_claim_bundle``, which calls the B05 operations directly
# rather than through main's wrappers. These cases therefore steer the B05
# services themselves; ``main``'s dependency factory still reads main's own
# attributes at call time, so every ``monkeypatch.setattr(orch_main, ...)``
# below keeps steering exactly what it steered before.
from orchestrator.services import (  # noqa: E402
    dispatch_credentials,
    job_start_bundle,
    job_workspace_authority,
    session_attach_payload,
    unit_claim_bundle,
)

UNIT_ID = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
POD_NAME = "stateless-agent-1"
POD_UID = "aaaaaaaa-1111-2222-3333-bbbbbbbbbbbb"
WORKSPACE_GENERATION = "11111111-1111-4111-8111-111111111111"
WORKSPACE_RUNTIME = "22222222-2222-4222-8222-222222222222"
WORKSPACE_FINGERPRINT = "SHA256:" + ("A" * 43)

LEASED_ROW = {
    "unit_kind": "session_turn",
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


class FakeDB:
    def __init__(self, *, run_queue_row, thread, job=None):
        self._row = run_queue_row
        self._thread = thread
        self._job = job
        self.conn = MagicMock()
        self.conn.transaction = lambda: _AsyncCM()

        async def _fetchrow(sql, *_args):
            if "FROM run_queue AS queue LEFT JOIN threads" in sql:
                return self._row
            if "SELECT status::text AS status, execution_lane, metadata" in sql:
                return self._thread
            if "SELECT state, lease_token, leased_by FROM run_queue" in sql:
                return self._row
            if (
                "SELECT accepted_result AS receipt FROM vm_workspace_recovery_requests"
                in sql
            ):
                return None
            if "SELECT * FROM worker_batch_attempts" in sql:
                return None
            raise AssertionError(f"unexpected fetchrow SQL: {sql}")

        self.conn.fetchrow = AsyncMock(side_effect=_fetchrow)

        async def _fetchval(sql, *_args):
            if "SELECT EXISTS (SELECT 1 FROM agents WHERE hostname" in sql:
                raise AssertionError(
                    "agents registration lookup must not be used for stateless "
                    "worker admission (claimant attestation replaces it)"
                )
            if "SELECT EXISTS (SELECT 1 FROM run_queue" in sql:
                # Final exact-token recheck: the live lease is still ours.
                return True
            if "SELECT 1 FROM run_queue WHERE unit_id" in sql:
                # record_bundle_authorized queue lock: lease still current.
                return 1
            if "UPDATE worker_batch_attempts SET bundle_authorized_at" in sql:
                return 1
            if sql.strip().startswith("UPDATE threads"):
                # Session-turn credential-bound stamp.
                return UNIT_ID
            raise AssertionError(f"unexpected fetchval SQL: {sql}")

        self.conn.fetchval = AsyncMock(side_effect=_fetchval)
        self.datasource_lock_calls = []
        # Bundle assembly rechecks repository authority the same way it
        # rechecks the lease: nothing current here, so nothing to invalidate.
        self.managed_repository_authorities_are_current = AsyncMock(return_value=True)

    def acquire(self):
        return _AsyncCM(self.conn)

    async def get_thread(self, tid):
        return self._thread

    async def get_job(self, jid):
        return self._job

    def thread_datasource_lock(self, tid):
        self.datasource_lock_calls.append(tid)
        return _AsyncCM()


class MultiJobFakeDB(FakeDB):
    """Production-shaped parent/child reads for inherited worker bundles."""

    def __init__(self, *, run_queue_row, jobs):
        super().__init__(run_queue_row=run_queue_row, thread=None, job=None)
        self.jobs = {str(job["id"]): deepcopy(job) for job in jobs}
        self.adoption_calls = []
        self.adoption_reservations = []
        self._adoption_reservation_id = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"

    async def get_job(self, jid):
        job = self.jobs.get(str(jid))
        return deepcopy(job) if job is not None else None

    async def reserve_managed_repository_workspace_creation(self, owner_id, **kwargs):
        self.adoption_reservations.append(("reserve", str(owner_id), deepcopy(kwargs)))
        return {
            "id": self._adoption_reservation_id,
            "reservation_generation": 1,
            "claim_token": 1,
        }

    async def authorize_managed_repository_workspace_creation_runtime(
        self, owner_id, **kwargs
    ):
        self.adoption_reservations.append(
            ("authorize", str(owner_id), deepcopy(kwargs))
        )
        return True

    async def abort_managed_repository_workspace_creation_reservation(
        self, owner_id, **kwargs
    ):
        self.adoption_reservations.append(("abort", str(owner_id), deepcopy(kwargs)))
        return True

    async def settle_managed_repository_workspace_creation_reservation(
        self, owner_id, **kwargs
    ):
        self.adoption_reservations.append(("settle", str(owner_id), deepcopy(kwargs)))
        return True

    async def adopt_legacy_k8s_job_workspace_runtime(self, job_id, **kwargs):
        self.adoption_calls.append((str(job_id), deepcopy(kwargs)))
        current = self.jobs.get(str(job_id))
        if current is None:
            return False
        context = current.get("context") or {}
        config = current.get("config_override") or {}
        if not (
            str(current.get("status") or "") == kwargs["expected_status"]
            and current.get("execution_lane") == kwargs["expected_execution_lane"]
            and (
                str(current["parent_job_id"]) if current.get("parent_job_id") else None
            )
            == kwargs["expected_parent_job_id"]
            and context.get("_workspace_contract") == kwargs["expected_contract"]
            and context.get("workspace_backend") == kwargs["expected_legacy_backend"]
            and config.get("workspace") == kwargs["expected_workspace_config"]
            and context.get("workspace_container") == kwargs["expected_workspace"]
        ):
            return False
        current["context"] = {
            **context,
            "workspace_container": deepcopy(kwargs["adopted_workspace"]),
        }
        return True


def _thread(**over):
    thread = {
        "id": UNIT_ID,
        "user_id": "user-1",
        "project_id": None,
        "execution_lane": "stateless",
        "status": "created",
        "config_name": "session_base",
        "metadata": {
            "config_override": {
                "llm": {"model": "m"},
                "workspace": {"backend": "virtual"},
            }
        },
    }
    thread.update(over)
    return thread


def _patch(monkeypatch, orch_main, db, *, attach="SENTINEL"):
    monkeypatch.setattr(orch_main, "require_internal", AsyncMock())
    monkeypatch.setattr(orch_main, "postgres_db", db)
    monkeypatch.setattr(orch_main, "_thread_project_ids", AsyncMock(return_value=[]))
    monkeypatch.setattr(
        orch_main, "_thread_has_knowledge_scope", AsyncMock(return_value=False)
    )
    inject = AsyncMock(side_effect=lambda co, **kw: co)
    monkeypatch.setattr(
        dispatch_credentials, "inject_thread_dispatch_credentials", inject
    )
    assembly = AsyncMock(
        return_value=(
            {
                "thread_id": UNIT_ID,
                "config_override": {"llm": {"model": "m"}},
                "resolved_config": None,
                "project_ids": [],
                "datasources": None,
                "config_name": "session_base",
            }
            if attach == "SENTINEL"
            else attach
        )
    )
    monkeypatch.setattr(
        session_attach_payload, "assemble_session_attach_payload", assembly
    )
    return inject, assembly


def _worker_attestation(orch_main, **overrides):
    values = {
        "backing_id": ("k8s-pod:superhuman-remote-worker:" + WORKSPACE_GENERATION),
        "workspace_generation": WORKSPACE_GENERATION,
        "runtime_incarnation": WORKSPACE_RUNTIME,
        "ssh_host_key_fingerprint": WORKSPACE_FINGERPRINT,
        "host": "10.0.0.9",
        "pod_ip": "10.0.0.9",
        "port": 30022,
    }
    values.update(overrides)
    return orch_main.WorkspaceRuntimeAttestation(**values)


def _patch_worker_attestation(monkeypatch, orch_main, *attestations):
    if not attestations:
        exact = _worker_attestation(orch_main)
        attestations = (exact, exact)
    attest = AsyncMock(side_effect=attestations)
    monkeypatch.setattr(
        orch_main.container_provisioner,
        "attest_workspace_runtime",
        attest,
    )
    # Every pooled sandbox bundle also needs executor-Pod attestation; default
    # it to success so workspace-focused tests isolate their own refusal.
    # Failure-matrix tests override the claimant mock afterwards.
    _patch_worker_claimant(monkeypatch, orch_main)
    return attest


def _vm_worker_attestation(orch_main, **overrides):
    return _worker_attestation(
        orch_main,
        backing_id=f"k8s-vmi:{WORKSPACE_RUNTIME}",
        host="10.42.1.23",
        pod_ip="10.42.1.23",
        port=22,
        **overrides,
    )


def _patch_vm_worker_attestation(monkeypatch, orch_main, *attestations):
    if not attestations:
        exact = _vm_worker_attestation(orch_main)
        attestations = (exact, exact)
    attest = AsyncMock(side_effect=attestations)
    monkeypatch.setattr(
        orch_main.vm_provisioner,
        "attest_workspace_runtime",
        attest,
    )
    _patch_worker_claimant(monkeypatch, orch_main)
    return attest


def _patch_worker_claimant(monkeypatch, orch_main, attest=None):
    """Inject a successful pooled-executor attestation for worker tests.

    Returns the mock so failure cases can override its side effect to a
    403/503 refusal. The mock is installed through the dependencies factory
    so both direct service calls and route-level apps observe it.
    """
    import dataclasses

    mock = attest if attest is not None else AsyncMock(return_value=None)
    original = orch_main._unit_claim_bundle_dependencies

    def _factory():
        deps = original()
        return dataclasses.replace(deps, attest_stateless_claimant=mock)

    monkeypatch.setattr(orch_main, "_unit_claim_bundle_dependencies", _factory)
    return mock


def _worker_job_context(container, **extra):
    return {
        "_workspace_contract": {
            "version": 1,
            "requested_backend": "sandbox",
            "assigned_backend": "sandbox",
            "assignment_source": "test",
        },
        "workspace_container": {
            **container,
            "_runtime_incarnation": WORKSPACE_RUNTIME,
        },
        **extra,
    }


def _worker_vm_context(vm, **extra):
    return {
        "_workspace_contract": {
            "version": 1,
            "requested_backend": "vm",
            "assigned_backend": "vm",
            "assignment_source": "test",
        },
        "vm": vm,
        **extra,
    }


def _ready_worker_vm(**overrides):
    vm = {
        "status": "ready",
        "provision_generation": WORKSPACE_GENERATION,
        "identity_authenticated": True,
        "identity_provision_generation": WORKSPACE_GENERATION,
        "vm_uid": "admitted-vm-uid",
        "active_pod_uid": WORKSPACE_RUNTIME,
        "ssh_host_key_fingerprint": WORKSPACE_FINGERPRINT,
        "ssh_ready_source": "provisioner_probe",
        "ssh_host": "10.42.1.22",
        "pod_ip": "10.42.1.22",
        "ssh_port": 22,
    }
    vm.update(overrides)
    return vm


def _adopted_worker_container(orch_main, **overrides):
    attestation = _worker_attestation(orch_main, **overrides)
    return {
        "status": "ready",
        "provisioner": "k8s",
        "host": attestation.host,
        "pod_ip": attestation.pod_ip,
        "port": attestation.port,
        "_runtime_incarnation": attestation.runtime_incarnation,
        "_legacy_k8s_runtime_adoption": {
            "version": 1,
            "runtime_incarnation": attestation.runtime_incarnation,
            "workspace_generation": attestation.workspace_generation,
            "ssh_host_key_fingerprint": (attestation.ssh_host_key_fingerprint),
        },
    }


@pytest.mark.asyncio
async def test_happy_path_returns_watermarks_and_shared_assembly(monkeypatch):
    from orchestrator import main as orch_main

    db = FakeDB(run_queue_row=dict(LEASED_ROW), thread=_thread())
    inject, assembly = _patch(monkeypatch, orch_main, db)

    out = await unit_claim_bundle.claim_bundle_for_unit(
        UNIT_ID,
        lease_token=7,
        pod_name=POD_NAME,
        pod_uid=POD_UID,
        dependencies=orch_main._unit_claim_bundle_dependencies(),
    )

    # One SELECT validated the lease AND carried the watermarks.
    assert db.conn.fetchrow.await_count == 3
    assert "run_queue" in db.conn.fetchrow.await_args_list[0].args[0]
    assert out["unit_id"] == UNIT_ID
    assert out["thread_id"] == UNIT_ID
    assert out["unit_kind"] == "session_turn"
    assert out["execution_lane"] == "stateless"
    assert out["watermarks"] == {"input_seq": 41, "consumed_seq": 12}
    # attach came from the ONE shared assembly, called under the datasource
    # lock with the credential-injected override + canonical config name.
    assembly.assert_awaited_once()
    assert assembly.await_args.args == (UNIT_ID,)
    assert assembly.await_args.kwargs["config_name"]  # canonicalized, non-empty
    inject.assert_awaited_once()
    assert db.datasource_lock_calls == [UNIT_ID]
    assert out["attach"]["thread_id"] == UNIT_ID
    assert set(out["attach"]) == {
        "thread_id",
        "config_override",
        "resolved_config",
        "project_ids",
        "datasources",
        "config_name",
    }


@pytest.mark.asyncio
async def test_session_bundle_stolen_during_slow_assembly_is_rejected(monkeypatch):
    """No attach credentials cross the response boundary after a token steal."""

    from orchestrator import main as orch_main

    db = FakeDB(run_queue_row=dict(LEASED_ROW), thread=_thread())
    _, assembly = _patch(monkeypatch, orch_main, db)
    entered = asyncio.Event()
    release = asyncio.Event()

    async def _slow_assembly(*_args, **_kwargs):
        entered.set()
        await release.wait()
        return {"thread_id": UNIT_ID, "datasources": {"secret": "in-flight"}}

    assembly.side_effect = _slow_assembly
    request_task = asyncio.create_task(
        unit_claim_bundle.claim_bundle_for_unit(
            UNIT_ID,
            lease_token=7,
            pod_name=POD_NAME,
            pod_uid=POD_UID,
            dependencies=orch_main._unit_claim_bundle_dependencies(),
        )
    )
    await asyncio.wait_for(entered.wait(), timeout=2)

    # The reaper/successor owns a newer token by the time assembly returns.
    async def _stolen_fetchval(sql, *_args):
        return None

    db.conn.fetchval.side_effect = _stolen_fetchval
    release.set()

    with pytest.raises(HTTPException) as exc:
        await request_task

    assert exc.value.status_code == 403
    assert exc.value.detail == "Lease validation failed"
    assert db.conn.fetchval.await_count == 1


@pytest.mark.asyncio
async def test_protected_cloud_flip_during_assembly_blocks_final_credentials(
    monkeypatch,
):
    from orchestrator import main as orch_main

    db = FakeDB(run_queue_row=dict(LEASED_ROW), thread=_thread())
    _, assembly = _patch(monkeypatch, orch_main, db)
    entered = asyncio.Event()
    release = asyncio.Event()

    async def _slow_assembly(*_args, **_kwargs):
        entered.set()
        await release.wait()
        return {"thread_id": UNIT_ID, "datasources": {"secret": "in-flight"}}

    assembly.side_effect = _slow_assembly
    request_task = asyncio.create_task(
        unit_claim_bundle.claim_bundle_for_unit(
            UNIT_ID,
            lease_token=7,
            pod_name=POD_NAME,
            pod_uid=POD_UID,
            dependencies=orch_main._unit_claim_bundle_dependencies(),
        )
    )
    await asyncio.wait_for(entered.wait(), timeout=2)
    db._thread["metadata"] = {
        **db._thread["metadata"],
        "protected_cloud": True,
    }
    release.set()

    with pytest.raises(HTTPException) as exc:
        await request_task
    assert exc.value.status_code == 403
    assert exc.value.detail == "Lease validation failed"


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["enabled", "conference"])
@pytest.mark.parametrize("value", [None, 0, "", [], {}, "yes", 1, True])
async def test_malformed_or_pinned_session_class_refuses_claim_credentials(
    monkeypatch,
    field,
    value,
):
    from orchestrator import main as orch_main

    thread = _thread()
    thread["metadata"]["config_override"]["officer"] = {field: value}
    db = FakeDB(run_queue_row=dict(LEASED_ROW), thread=thread)
    _patch(monkeypatch, orch_main, db)

    with pytest.raises(HTTPException) as exc:
        await unit_claim_bundle.claim_bundle_for_unit(
            UNIT_ID,
            lease_token=7,
            pod_name=POD_NAME,
            pod_uid=POD_UID,
            dependencies=orch_main._unit_claim_bundle_dependencies(),
        )
    assert exc.value.status_code in {403, 409}
    db.conn.fetchval.assert_not_awaited()


@pytest.mark.asyncio
async def test_session_class_flip_during_assembly_blocks_final_credentials(
    monkeypatch,
):
    from orchestrator import main as orch_main

    db = FakeDB(run_queue_row=dict(LEASED_ROW), thread=_thread())
    _, assembly = _patch(monkeypatch, orch_main, db)
    entered = asyncio.Event()
    release = asyncio.Event()

    async def _slow_assembly(*_args, **_kwargs):
        entered.set()
        await release.wait()
        return {"thread_id": UNIT_ID, "datasources": {"secret": "in-flight"}}

    assembly.side_effect = _slow_assembly
    request_task = asyncio.create_task(
        unit_claim_bundle.claim_bundle_for_unit(
            UNIT_ID,
            lease_token=7,
            pod_name=POD_NAME,
            pod_uid=POD_UID,
            dependencies=orch_main._unit_claim_bundle_dependencies(),
        )
    )
    await asyncio.wait_for(entered.wait(), timeout=2)
    db._thread["metadata"]["config_override"]["officer"] = {"enabled": "yes"}
    release.set()

    with pytest.raises(HTTPException) as exc:
        await request_task
    assert exc.value.status_code == 403
    assert exc.value.detail == "Lease validation failed"


@pytest.mark.asyncio
async def test_token_mismatch_and_not_leased_are_one_generic_403(monkeypatch):
    from orchestrator import main as orch_main

    # Wrong token on a live lease.
    db = FakeDB(run_queue_row=dict(LEASED_ROW), thread=_thread())
    _patch(monkeypatch, orch_main, db)
    with pytest.raises(HTTPException) as exc_token:
        await unit_claim_bundle.claim_bundle_for_unit(
            UNIT_ID,
            lease_token=6,
            pod_name=POD_NAME,
            pod_uid=POD_UID,
            dependencies=orch_main._unit_claim_bundle_dependencies(),
        )
    assert exc_token.value.status_code == 403

    # Right token but the row is no longer leased (stolen/completed).
    row = dict(LEASED_ROW, state="queued")
    db2 = FakeDB(run_queue_row=row, thread=_thread())
    _patch(monkeypatch, orch_main, db2)
    with pytest.raises(HTTPException) as exc_state:
        await unit_claim_bundle.claim_bundle_for_unit(
            UNIT_ID,
            lease_token=7,
            pod_name=POD_NAME,
            pod_uid=POD_UID,
            dependencies=orch_main._unit_claim_bundle_dependencies(),
        )
    assert exc_state.value.status_code == 403

    # Single generic detail: the two cases must be indistinguishable.
    assert exc_token.value.detail == exc_state.value.detail


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "marker_key",
    [
        "_stateless_workspace_retirement_pending",
        "_stateless_claim_retirement",
        "_stateless_claim_loss_hold",
        "_stateless_claim_losses",
    ],
)
@pytest.mark.parametrize("value", [None, False, 0, "", [], {}])
async def test_present_falsey_stop_marker_refuses_credentials(
    monkeypatch, marker_key, value
):
    from orchestrator import main as orch_main

    row = dict(
        LEASED_ROW,
        thread_metadata={marker_key: value},
    )
    db = FakeDB(run_queue_row=row, thread=_thread())
    _patch(monkeypatch, orch_main, db)

    with pytest.raises(HTTPException) as exc:
        await unit_claim_bundle.claim_bundle_for_unit(
            UNIT_ID,
            lease_token=7,
            pod_name=POD_NAME,
            pod_uid=POD_UID,
            dependencies=orch_main._unit_claim_bundle_dependencies(),
        )
    assert exc.value.status_code == 403


@pytest.mark.asyncio
async def test_absent_unit_row_404(monkeypatch):
    from orchestrator import main as orch_main

    db = FakeDB(run_queue_row=None, thread=_thread())
    _patch(monkeypatch, orch_main, db)
    with pytest.raises(HTTPException) as exc:
        await unit_claim_bundle.claim_bundle_for_unit(
            UNIT_ID,
            lease_token=7,
            pod_name=POD_NAME,
            pod_uid=POD_UID,
            dependencies=orch_main._unit_claim_bundle_dependencies(),
        )
    assert exc.value.status_code == 404

    # Malformed unit id short-circuits to the same 404 (no DataError 500).
    with pytest.raises(HTTPException) as exc2:
        await unit_claim_bundle.claim_bundle_for_unit(
            "not-a-uuid",
            lease_token=7,
            pod_name=POD_NAME,
            pod_uid=POD_UID,
            dependencies=orch_main._unit_claim_bundle_dependencies(),
        )
    assert exc2.value.status_code == 404


@pytest.mark.asyncio
async def test_wrong_lane_and_unknown_unit_kind_409(monkeypatch):
    from orchestrator import main as orch_main

    # Pinned-lane thread behind a leased session unit.
    db = FakeDB(run_queue_row=dict(LEASED_ROW), thread=_thread(execution_lane="pinned"))
    _patch(monkeypatch, orch_main, db)
    with pytest.raises(HTTPException) as exc:
        await unit_claim_bundle.claim_bundle_for_unit(
            UNIT_ID,
            lease_token=7,
            pod_name=POD_NAME,
            pod_uid=POD_UID,
            dependencies=orch_main._unit_claim_bundle_dependencies(),
        )
    assert exc.value.status_code == 409

    # Vanished thread → same 409 family.
    db2 = FakeDB(run_queue_row=dict(LEASED_ROW), thread=None)
    _patch(monkeypatch, orch_main, db2)
    with pytest.raises(HTTPException) as exc2:
        await unit_claim_bundle.claim_bundle_for_unit(
            UNIT_ID,
            lease_token=7,
            pod_name=POD_NAME,
            pod_uid=POD_UID,
            dependencies=orch_main._unit_claim_bundle_dependencies(),
        )
    assert exc2.value.status_code == 409

    # Unknown unit kinds carry no attach bundle.
    row = dict(LEASED_ROW, unit_kind="unknown")
    db3 = FakeDB(run_queue_row=row, thread=_thread())
    _patch(monkeypatch, orch_main, db3)
    with pytest.raises(HTTPException) as exc3:
        await unit_claim_bundle.claim_bundle_for_unit(
            UNIT_ID,
            lease_token=7,
            pod_name=POD_NAME,
            pod_uid=POD_UID,
            dependencies=orch_main._unit_claim_bundle_dependencies(),
        )
    assert exc3.value.status_code == 409


@pytest.mark.asyncio
@pytest.mark.parametrize("refused", [False, True])
async def test_bg_task_bundle_uses_cloud_only_builder(monkeypatch, refused):
    from orchestrator import main as orch_main
    from orchestrator.services import cloud_push_recovery

    db = FakeDB(run_queue_row=dict(LEASED_ROW, unit_kind="bg_task"), thread=_thread())
    _patch(monkeypatch, orch_main, db)
    result = {"unit_id": UNIT_ID, "unit_kind": "bg_task", "obsolete": True}
    builder = AsyncMock(
        return_value=result,
        side_effect=(
            cloud_push_recovery.CloudPushBundleRefused("private refusal reason")
            if refused
            else None
        ),
    )
    monkeypatch.setattr(cloud_push_recovery, "build_cloud_push_bundle", builder)
    attach = AsyncMock(side_effect=AssertionError("session attach is forbidden"))
    monkeypatch.setattr(
        session_attach_payload, "assemble_session_attach_payload", attach
    )
    if refused:
        with pytest.raises(HTTPException) as exc:
            await unit_claim_bundle.claim_bundle_for_unit(
                UNIT_ID,
                lease_token=7,
                pod_name=POD_NAME,
                pod_uid=POD_UID,
                dependencies=orch_main._unit_claim_bundle_dependencies(),
            )
        assert exc.value.status_code == 403
        assert exc.value.detail == "Lease validation failed"
    else:
        assert (
            await unit_claim_bundle.claim_bundle_for_unit(
                UNIT_ID,
                lease_token=7,
                pod_name=POD_NAME,
                pod_uid=POD_UID,
                dependencies=orch_main._unit_claim_bundle_dependencies(),
            )
            == result
        )
    builder.assert_awaited_once_with(
        db,
        unit_id=UNIT_ID,
        lease_token=7,
        pod_name=POD_NAME,
        pod_uid=POD_UID,
        resolve_workspace=ANY,
    )
    assert callable(builder.await_args.kwargs["resolve_workspace"])
    attach.assert_not_awaited()


@pytest.mark.asyncio
async def test_worker_bundle_reuses_job_start_builder_and_rechecks_lease(monkeypatch):
    from orchestrator import main as orch_main

    row = dict(LEASED_ROW, unit_kind="worker_batch")
    job = {
        "id": UNIT_ID,
        "execution_lane": "stateless",
        "config_override": {"workspace": {"backend": "sandbox"}},
        "context": _worker_job_context(
            {
                "status": "ready",
                "provisioner": "k8s",
                "pod_ip": "10.0.0.8",
            },
            worker_batch_target_wall_seconds=360,
            worker_batch_iteration_cap=9,
        ),
    }
    db = FakeDB(run_queue_row=row, thread=None, job=job)
    monkeypatch.setattr(orch_main, "require_internal", AsyncMock())
    monkeypatch.setattr(orch_main, "postgres_db", db)
    built = orch_main.JobStartRequest(job_id=UNIT_ID, description="work")
    builder = AsyncMock(return_value=built)
    monkeypatch.setattr(job_start_bundle, "build_job_start_request", builder)
    inherit = AsyncMock(return_value=("proceed", None))
    monkeypatch.setattr(
        job_workspace_authority, "resolve_subjob_inherited_workspace", inherit
    )
    attest = _patch_worker_attestation(monkeypatch, orch_main)

    out = await unit_claim_bundle.claim_bundle_for_unit(
        UNIT_ID,
        lease_token=7,
        pod_name=POD_NAME,
        pod_uid=POD_UID,
        dependencies=orch_main._unit_claim_bundle_dependencies(),
    )

    # R1.B06: the bundle calls the B05 operation directly now, so the call
    # also carries its injected ``dependencies``. Assert the subject, not
    # the plumbing.
    inherit.assert_awaited_once()
    assert inherit.await_args.args == (job,)
    builder.assert_awaited_once()
    attested_job = builder.await_args.args[0]
    assert attested_job is not job
    assert attested_job["context"]["workspace_container"] == {
        "status": "ready",
        "provisioner": "k8s",
        "pod_ip": "10.0.0.9",
        "host": "10.0.0.9",
        "port": 30022,
        "_runtime_incarnation": WORKSPACE_RUNTIME,
    }
    assert attested_job["config_override"]["workspace"]["remote"]["host"] == (
        "10.0.0.9"
    )
    # R1.B06: the bundle calls job_start_bundle directly, so the call also
    # carries its injected ``dependencies``. The subject of this assertion
    # is the flag.
    assert builder.await_args.kwargs["persist_dispatch_state"] is False
    assert attest.await_count == 2
    assert attest.await_args_list[0].args[0] == WorkspaceOwner.job(UNIT_ID)
    lease_reads = [
        call
        for call in db.conn.fetchval.await_args_list
        if "SELECT EXISTS (SELECT 1 FROM run_queue" in call.args[0]
    ]
    assert len(lease_reads) == 1
    assert lease_reads[0].args[1:] == (UNIT_ID, 7)
    assert out == {
        "unit_id": UNIT_ID,
        "job_id": UNIT_ID,
        "unit_kind": "worker_batch",
        "execution_lane": "stateless",
        "job": {
            "job_id": UNIT_ID,
            "description": "work",
            "config_name": "worker_base",
            "workspace_generation": WORKSPACE_GENERATION,
            "workspace_runtime_incarnation": WORKSPACE_RUNTIME,
            "workspace_ssh_host_key_fingerprint": WORKSPACE_FINGERPRINT,
            "workspace_owner_kind": "job",
            "workspace_owner_id": UNIT_ID,
        },
        "batch": {
            "target_wall_seconds": 360.0,
            "iteration_cap": 9,
            "min_wall_seconds": 300.0,
        },
    }


@pytest.mark.asyncio
async def test_worker_vm_bundle_uses_attested_endpoint_and_stamps_host_key_pin(
    monkeypatch,
):
    from orchestrator import main as orch_main

    monkeypatch.setenv("VM_MODE", "same-cluster")
    row = dict(LEASED_ROW, unit_kind="worker_batch")
    job = {
        "id": UNIT_ID,
        "execution_lane": "stateless",
        "config_override": {"workspace": {"backend": "vm"}},
        "context": _worker_vm_context(
            _ready_worker_vm(),
            worker_batch_target_wall_seconds=420,
        ),
    }
    db = FakeDB(run_queue_row=row, thread=None, job=job)
    monkeypatch.setattr(orch_main, "require_internal", AsyncMock())
    monkeypatch.setattr(orch_main, "postgres_db", db)
    builder = AsyncMock(
        return_value=orch_main.JobStartRequest(job_id=UNIT_ID, description="vm work")
    )
    monkeypatch.setattr(job_start_bundle, "build_job_start_request", builder)
    monkeypatch.setattr(
        job_workspace_authority,
        "resolve_subjob_inherited_workspace",
        AsyncMock(return_value=("proceed", None)),
    )
    attest = _patch_vm_worker_attestation(monkeypatch, orch_main)

    out = await unit_claim_bundle.claim_bundle_for_unit(
        UNIT_ID,
        lease_token=7,
        pod_name=POD_NAME,
        pod_uid=POD_UID,
        dependencies=orch_main._unit_claim_bundle_dependencies(),
    )

    assert attest.await_count == 2
    assert all(call.args == (UNIT_ID,) for call in attest.await_args_list)
    attested_job = builder.await_args.args[0]
    exact_vm = attested_job["context"]["vm"]
    assert exact_vm["ssh_host"] == "10.42.1.23"
    assert exact_vm["pod_ip"] == "10.42.1.23"
    assert exact_vm["ssh_port"] == 22
    assert exact_vm["provision_generation"] == WORKSPACE_GENERATION
    assert exact_vm["active_pod_uid"] == WORKSPACE_RUNTIME
    assert exact_vm["ssh_host_key_fingerprint"] == WORKSPACE_FINGERPRINT
    remote = attested_job["config_override"]["workspace"]["remote"]
    assert remote["host"] == "10.42.1.23"
    assert remote["port"] == 22
    assert out["job"]["workspace_generation"] == WORKSPACE_GENERATION
    assert out["job"]["workspace_runtime_incarnation"] == WORKSPACE_RUNTIME
    assert out["job"]["workspace_ssh_host_key_fingerprint"] == WORKSPACE_FINGERPRINT
    assert out["batch"]["target_wall_seconds"] == 420.0


@pytest.mark.asyncio
async def test_worker_vm_bundle_refuses_external_topology(monkeypatch):
    from orchestrator import main as orch_main

    monkeypatch.setenv("VM_MODE", "external")
    row = dict(LEASED_ROW, unit_kind="worker_batch")
    job = {
        "id": UNIT_ID,
        "execution_lane": "stateless",
        "config_override": {"workspace": {"backend": "vm"}},
        "context": _worker_vm_context(_ready_worker_vm()),
    }
    db = FakeDB(run_queue_row=row, thread=None, job=job)
    monkeypatch.setattr(orch_main, "require_internal", AsyncMock())
    monkeypatch.setattr(orch_main, "postgres_db", db)
    _patch_worker_claimant(monkeypatch, orch_main)
    builder = AsyncMock()
    monkeypatch.setattr(job_start_bundle, "build_job_start_request", builder)

    with pytest.raises(HTTPException) as exc:
        await unit_claim_bundle.claim_bundle_for_unit(
            UNIT_ID,
            lease_token=7,
            pod_name=POD_NAME,
            pod_uid=POD_UID,
            dependencies=orch_main._unit_claim_bundle_dependencies(),
        )

    assert exc.value.status_code == 409
    assert exc.value.detail == "Job workspace contract is not stateless-compatible"
    builder.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "vm_updates",
    [
        {"identity_authenticated": False},
        {"ssh_host_key_fingerprint": None},
    ],
    ids=["not-ready-identity", "absent-pin"],
)
async def test_worker_vm_bundle_refuses_incomplete_ready_context(
    monkeypatch, vm_updates
):
    from orchestrator import main as orch_main

    monkeypatch.setenv("VM_MODE", "same-cluster")
    row = dict(LEASED_ROW, unit_kind="worker_batch")
    job = {
        "id": UNIT_ID,
        "execution_lane": "stateless",
        "config_override": {"workspace": {"backend": "vm"}},
        "context": _worker_vm_context(_ready_worker_vm(**vm_updates)),
    }
    db = FakeDB(run_queue_row=row, thread=None, job=job)
    monkeypatch.setattr(orch_main, "require_internal", AsyncMock())
    monkeypatch.setattr(orch_main, "postgres_db", db)
    builder = AsyncMock()
    monkeypatch.setattr(job_start_bundle, "build_job_start_request", builder)
    monkeypatch.setattr(
        job_workspace_authority,
        "resolve_subjob_inherited_workspace",
        AsyncMock(return_value=("proceed", None)),
    )
    attest = _patch_vm_worker_attestation(monkeypatch, orch_main)

    with pytest.raises(HTTPException) as exc:
        await unit_claim_bundle.claim_bundle_for_unit(
            UNIT_ID,
            lease_token=7,
            pod_name=POD_NAME,
            pod_uid=POD_UID,
            dependencies=orch_main._unit_claim_bundle_dependencies(),
        )

    assert exc.value.status_code == 409
    assert exc.value.detail == "Stateless worker VM workspace is not Kubernetes-ready"
    attest.assert_not_awaited()
    builder.assert_not_awaited()


@pytest.mark.asyncio
async def test_worker_bundle_stolen_during_assembly_is_rejected(monkeypatch):
    from orchestrator import main as orch_main

    row = dict(LEASED_ROW, unit_kind="worker_batch")
    job = {
        "id": UNIT_ID,
        "execution_lane": "stateless",
        "config_override": {"workspace": {"backend": "sandbox"}},
        "context": _worker_job_context(
            {
                "status": "ready",
                "provisioner": "k8s",
                "host": "workspace.example",
            }
        ),
    }
    db = FakeDB(run_queue_row=row, thread=None, job=job)

    async def _stolen_worker_fetchval(sql, *_args):
        if "SELECT EXISTS (SELECT 1 FROM run_queue" in sql:
            return False
        if "SELECT 1 FROM run_queue WHERE unit_id" in sql:
            return None
        if "UPDATE worker_batch_attempts SET bundle_authorized_at" in sql:
            return None
        raise AssertionError(f"unexpected fetchval SQL: {sql}")

    db.conn.fetchval.side_effect = _stolen_worker_fetchval
    monkeypatch.setattr(orch_main, "require_internal", AsyncMock())
    monkeypatch.setattr(orch_main, "postgres_db", db)
    monkeypatch.setattr(
        job_start_bundle,
        "build_job_start_request",
        AsyncMock(
            return_value=orch_main.JobStartRequest(
                job_id=UNIT_ID, description="secret-bearing"
            )
        ),
    )
    monkeypatch.setattr(
        job_workspace_authority,
        "resolve_subjob_inherited_workspace",
        AsyncMock(return_value=("proceed", None)),
    )
    _patch_worker_attestation(monkeypatch, orch_main)

    with pytest.raises(HTTPException) as exc:
        await unit_claim_bundle.claim_bundle_for_unit(
            UNIT_ID,
            lease_token=7,
            pod_name=POD_NAME,
            pod_uid=POD_UID,
            dependencies=orch_main._unit_claim_bundle_dependencies(),
        )

    assert exc.value.status_code == 403
    assert exc.value.detail == "Lease validation failed"


@pytest.mark.asyncio
async def test_worker_bundle_rejects_rotated_repository_authority(monkeypatch):
    """A repository authority rotated during assembly invalidates the bundle.

    The lease can still be exactly ours while the credentials the builder
    already baked into the bundle have been revoked underneath it, so this
    recheck is independent of the lease recheck above.
    """
    from orchestrator import main as orch_main

    row = dict(LEASED_ROW, unit_kind="worker_batch")
    job = {
        "id": UNIT_ID,
        "execution_lane": "stateless",
        "config_override": {"workspace": {"backend": "sandbox"}},
        "context": _worker_job_context(
            {
                "status": "ready",
                "provisioner": "k8s",
                "pod_ip": "10.0.0.8",
            }
        ),
    }
    db = FakeDB(run_queue_row=row, thread=None, job=job)
    db.managed_repository_authorities_are_current.return_value = False
    monkeypatch.setattr(orch_main, "require_internal", AsyncMock())
    monkeypatch.setattr(orch_main, "postgres_db", db)
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
            return_value=orch_main.JobStartRequest(
                job_id=UNIT_ID,
                description="secret-bearing",
                managed_repository_credentials=credentials,
            )
        ),
    )
    monkeypatch.setattr(
        job_workspace_authority,
        "resolve_subjob_inherited_workspace",
        AsyncMock(return_value=("proceed", None)),
    )
    _patch_worker_attestation(monkeypatch, orch_main)

    with pytest.raises(HTTPException) as exc:
        await unit_claim_bundle.claim_bundle_for_unit(
            UNIT_ID,
            lease_token=7,
            pod_name=POD_NAME,
            pod_uid=POD_UID,
            dependencies=orch_main._unit_claim_bundle_dependencies(),
        )

    assert exc.value.status_code == 409
    assert exc.value.detail == "Job repository authority changed during bundle assembly"
    # The lease was still exactly ours; only the authority moved.
    db.conn.fetchval.assert_awaited_once()
    db.managed_repository_authorities_are_current.assert_awaited_once_with(credentials)


@pytest.mark.asyncio
async def test_worker_bundle_rejects_workspace_drift_after_slow_assembly(monkeypatch):
    from orchestrator import main as orch_main

    row = dict(LEASED_ROW, unit_kind="worker_batch")
    job = {
        "id": UNIT_ID,
        "execution_lane": "stateless",
        "config_override": {"workspace": {"backend": "sandbox"}},
        "context": _worker_job_context(
            {
                "status": "ready",
                "provisioner": "k8s",
                "pod_ip": "10.0.0.8",
            }
        ),
    }
    db = FakeDB(run_queue_row=row, thread=None, job=job)
    monkeypatch.setattr(orch_main, "require_internal", AsyncMock())
    monkeypatch.setattr(orch_main, "postgres_db", db)
    monkeypatch.setattr(
        job_start_bundle,
        "build_job_start_request",
        AsyncMock(
            return_value=orch_main.JobStartRequest(job_id=UNIT_ID, description="x")
        ),
    )
    monkeypatch.setattr(
        job_workspace_authority,
        "resolve_subjob_inherited_workspace",
        AsyncMock(return_value=("proceed", None)),
    )
    initial = _worker_attestation(orch_main)
    changed = _worker_attestation(
        orch_main,
        runtime_incarnation="33333333-3333-4333-8333-333333333333",
    )
    _patch_worker_attestation(monkeypatch, orch_main, initial, changed)

    with pytest.raises(HTTPException) as exc:
        await unit_claim_bundle.claim_bundle_for_unit(
            UNIT_ID,
            lease_token=7,
            pod_name=POD_NAME,
            pod_uid=POD_UID,
            dependencies=orch_main._unit_claim_bundle_dependencies(),
        )

    assert exc.value.status_code == 409
    assert exc.value.detail == "Stateless worker workspace authority unavailable"
    db.conn.fetchval.assert_not_awaited()


@pytest.mark.asyncio
async def test_inherited_worker_attests_parent_but_keeps_child_tmux_owner(monkeypatch):
    from orchestrator import main as orch_main

    parent_id = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
    row = dict(LEASED_ROW, unit_kind="worker_batch")
    job = {
        "id": UNIT_ID,
        "parent_job_id": parent_id,
        "execution_lane": "stateless",
        "config_override": {"workspace": {"backend": "sandbox"}},
        "context": _worker_job_context(
            {
                "status": "ready",
                "provisioner": "k8s",
                "host": "stale.example",
            },
            inherits_parent_workspace=True,
        ),
    }
    db = FakeDB(run_queue_row=row, thread=None, job=job)
    monkeypatch.setattr(orch_main, "require_internal", AsyncMock())
    monkeypatch.setattr(orch_main, "postgres_db", db)
    monkeypatch.setattr(
        job_workspace_authority,
        "resolve_subjob_inherited_workspace",
        AsyncMock(return_value=("proceed", None)),
    )
    builder = AsyncMock(
        return_value=orch_main.JobStartRequest(job_id=UNIT_ID, description="child")
    )
    monkeypatch.setattr(job_start_bundle, "build_job_start_request", builder)
    attest = _patch_worker_attestation(monkeypatch, orch_main)

    out = await unit_claim_bundle.claim_bundle_for_unit(
        UNIT_ID,
        lease_token=7,
        pod_name=POD_NAME,
        pod_uid=POD_UID,
        dependencies=orch_main._unit_claim_bundle_dependencies(),
    )

    assert attest.await_count == 2
    assert all(
        call.args[0] == WorkspaceOwner.job(parent_id) for call in attest.await_args_list
    )
    assert builder.await_args.args[0]["context"]["workspace_container"]["host"] == (
        "10.0.0.9"
    )
    assert out["job"]["job_id"] == UNIT_ID
    assert out["job"]["workspace_owner_kind"] == "job"
    assert out["job"]["workspace_owner_id"] == parent_id


@pytest.mark.asyncio
async def test_pre_0175_inherited_worker_final_reread_converges_parent(monkeypatch):
    from orchestrator import main as orch_main

    parent_id = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
    row = dict(LEASED_ROW, unit_kind="worker_batch")
    parent = {
        "id": parent_id,
        "status": "waiting",
        "execution_lane": "pinned",
        "config_override": {"workspace": {"backend": "container"}},
        # Exact previous-release parent runtime: neither contract nor UID.
        "context": {
            "workspace_container": {
                "status": "ready",
                "provisioner": "k8s",
                "host": "10.0.0.7",
                "pod_ip": "10.0.0.7",
                "port": 30022,
            }
        },
    }
    child = {
        "id": UNIT_ID,
        "parent_job_id": parent_id,
        "status": "created",
        "execution_lane": "stateless",
        "config_override": {"workspace": {"backend": "container"}},
        # Exact pre-0175 child shape: the inherited snapshot has no contract,
        # adoption marker or Pod UID. Only the live parent may supply them.
        "context": {
            "inherits_parent_workspace": True,
            "workspace_container": {
                "status": "ready",
                "provisioner": "k8s",
                "pod_ip": "10.0.0.7",
                "port": 30022,
            },
        },
    }
    db = MultiJobFakeDB(run_queue_row=row, jobs=[parent, child])
    monkeypatch.setattr(orch_main, "require_internal", AsyncMock())
    monkeypatch.setattr(orch_main, "postgres_db", db)
    builder = AsyncMock(
        return_value=orch_main.JobStartRequest(job_id=UNIT_ID, description="child")
    )
    monkeypatch.setattr(job_start_bundle, "build_job_start_request", builder)
    exact = _worker_attestation(orch_main)
    attest = _patch_worker_attestation(
        monkeypatch, orch_main, exact, exact, exact, exact, exact, exact, exact
    )

    out = await unit_claim_bundle.claim_bundle_for_unit(
        UNIT_ID,
        lease_token=7,
        pod_name=POD_NAME,
        pod_uid=POD_UID,
        dependencies=orch_main._unit_claim_bundle_dependencies(),
    )

    builder.assert_awaited_once()
    assert any(
        "UPDATE worker_batch_attempts SET bundle_authorized_at" in call.args[0]
        for call in db.conn.fetchval.await_args_list
    )
    assert out["job"]["workspace_owner_id"] == parent_id
    assert out["job"]["workspace_runtime_incarnation"] == WORKSPACE_RUNTIME
    assert attest.await_count == 7
    assert all(
        call.args[0] == WorkspaceOwner.job(parent_id) for call in attest.await_args_list
    )
    # The historical child row remains a snapshot; both the initial and final
    # contract checks obtained current authority from the parent overlay.
    stored_child = await db.get_job(UNIT_ID)
    assert "_runtime_incarnation" not in stored_child["context"]["workspace_container"]
    stored_parent = await db.get_job(parent_id)
    assert (
        stored_parent["context"]["workspace_container"]["_runtime_incarnation"]
        == WORKSPACE_RUNTIME
    )
    assert len(db.adoption_calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["replacement", "tier"], ids=["pod", "tier"])
async def test_inherited_worker_rejects_parent_change_after_assembly(
    monkeypatch, change
):
    from orchestrator import main as orch_main

    parent_id = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
    row = dict(LEASED_ROW, unit_kind="worker_batch")
    parent = {
        "id": parent_id,
        "status": "waiting",
        "execution_lane": "pinned",
        "config_override": {"workspace": {"backend": "sandbox"}},
        "context": _worker_job_context(_adopted_worker_container(orch_main)),
    }
    child = {
        "id": UNIT_ID,
        "parent_job_id": parent_id,
        "status": "created",
        "execution_lane": "stateless",
        "config_override": {"workspace": {"backend": "sandbox"}},
        "context": {
            "inherits_parent_workspace": True,
            "workspace_container": {
                "status": "ready",
                "provisioner": "k8s",
                "pod_ip": "10.0.0.7",
                "port": 30022,
            },
        },
    }
    db = MultiJobFakeDB(run_queue_row=row, jobs=[parent, child])
    monkeypatch.setattr(orch_main, "require_internal", AsyncMock())
    monkeypatch.setattr(orch_main, "postgres_db", db)
    builder = AsyncMock(
        return_value=orch_main.JobStartRequest(job_id=UNIT_ID, description="child")
    )
    monkeypatch.setattr(job_start_bundle, "build_job_start_request", builder)
    predecessor = _worker_attestation(orch_main)
    replacement = _worker_attestation(
        orch_main,
        runtime_incarnation="33333333-3333-4333-8333-333333333333",
        workspace_generation="44444444-4444-4444-8444-444444444444",
        host="10.0.0.10",
        pod_ip="10.0.0.10",
    )
    calls = 0

    async def attest(owner):
        nonlocal calls
        assert owner == WorkspaceOwner.job(parent_id)
        calls += 1
        if calls == 4:
            if change == "replacement":
                db.jobs[parent_id]["context"]["workspace_container"] = (
                    _adopted_worker_container(
                        orch_main,
                        runtime_incarnation=replacement.runtime_incarnation,
                        workspace_generation=replacement.workspace_generation,
                        host=replacement.host,
                        pod_ip=replacement.pod_ip,
                    )
                )
            else:
                db.jobs[parent_id]["config_override"] = {"workspace": {"backend": "vm"}}
                db.jobs[parent_id]["context"] = {
                    "_workspace_contract": {
                        "version": 1,
                        "requested_backend": "vm",
                        "assigned_backend": "vm",
                        "assignment_source": "operator",
                    },
                    "vm": {
                        "status": "ready",
                        "ssh_host": "vm.internal",
                        "ssh_port": 22,
                        "provision_generation": (
                            "55555555-5555-4555-8555-555555555555"
                        ),
                    },
                }
            # The replacement/tier transition begins just after the final live
            # attestation used by assembly, forcing the database reread fence.
            return predecessor
        return replacement if change == "replacement" and calls >= 5 else predecessor

    monkeypatch.setattr(
        orch_main.container_provisioner,
        "attest_workspace_runtime",
        AsyncMock(side_effect=attest),
    )
    _patch_worker_claimant(monkeypatch, orch_main)

    with pytest.raises(HTTPException) as exc:
        await unit_claim_bundle.claim_bundle_for_unit(
            UNIT_ID,
            lease_token=7,
            pod_name=POD_NAME,
            pod_uid=POD_UID,
            dependencies=orch_main._unit_claim_bundle_dependencies(),
        )

    assert exc.value.status_code == 409
    assert exc.value.detail == "Job workspace contract changed during bundle assembly"
    builder.assert_awaited_once()
    db.conn.fetchval.assert_not_awaited()


@pytest.mark.asyncio
async def test_stateless_bundle_refusal_never_mutates_job_status(monkeypatch):
    from orchestrator import main as orch_main

    db = MagicMock()
    db.update_job_status = AsyncMock()
    monkeypatch.setattr(orch_main, "postgres_db", db)
    monkeypatch.setattr(
        orch_main,
        "_resolve_authorized_job_datasources",
        AsyncMock(side_effect=HTTPException(status_code=409, detail="revoked")),
    )

    built = await job_start_bundle.build_job_start_request(
        {
            "id": UNIT_ID,
            "description": "work",
            "context": {},
            "config_override": {"workspace": {"backend": "sandbox"}},
        },
        persist_dispatch_state=False,
        dependencies=orch_main._job_start_bundle_dependencies(),
    )

    assert built is None
    db.update_job_status.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("backend", ["sandbox", "vm", "future-tier", None])
async def test_non_lite_workspace_refused_before_attach_assembly(monkeypatch, backend):
    from orchestrator import main as orch_main

    metadata = (
        {"config_override": {"workspace": {"backend": backend}}}
        if backend is not None
        else {}
    )
    db = FakeDB(
        run_queue_row=dict(LEASED_ROW),
        thread=_thread(metadata=metadata),
    )
    inject, assembly = _patch(monkeypatch, orch_main, db)

    with pytest.raises(HTTPException) as exc:
        await unit_claim_bundle.claim_bundle_for_unit(
            UNIT_ID,
            lease_token=7,
            pod_name=POD_NAME,
            pod_uid=POD_UID,
            dependencies=orch_main._unit_claim_bundle_dependencies(),
        )

    assert exc.value.status_code == 409
    assert "virtual/none" in str(exc.value.detail)
    inject.assert_not_awaited()
    assembly.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "physical_evidence",
    [
        {"workspace_container": {"status": "ready", "pod_ip": "10.0.0.8"}},
        {"vm": {"status": "provisioning", "provision_generation": "g1"}},
    ],
    ids=["sandbox-upgrade", "vm-upgrade"],
)
async def test_upgraded_lite_claim_refused_before_credentials_or_assembly(
    monkeypatch, physical_evidence
):
    from orchestrator import main as orch_main

    metadata = {
        "config_override": {"workspace": {"backend": "virtual"}},
        **physical_evidence,
    }
    db = FakeDB(
        run_queue_row=dict(LEASED_ROW),
        thread=_thread(metadata=metadata),
    )
    inject, assembly = _patch(monkeypatch, orch_main, db)

    with pytest.raises(HTTPException) as exc:
        await unit_claim_bundle.claim_bundle_for_unit(
            UNIT_ID,
            lease_token=7,
            pod_name=POD_NAME,
            pod_uid=POD_UID,
            dependencies=orch_main._unit_claim_bundle_dependencies(),
        )

    assert exc.value.status_code == 409
    assert "virtual/none" in str(exc.value.detail)
    inject.assert_not_awaited()
    assembly.assert_not_awaited()
    assert db.datasource_lock_calls == []


@pytest.mark.asyncio
async def test_assembly_refusal_is_generic_409(monkeypatch):
    from orchestrator import main as orch_main

    db = FakeDB(run_queue_row=dict(LEASED_ROW), thread=_thread())
    _patch(monkeypatch, orch_main, db, attach=None)
    with pytest.raises(HTTPException) as exc:
        await unit_claim_bundle.claim_bundle_for_unit(
            UNIT_ID,
            lease_token=7,
            pod_name=POD_NAME,
            pod_uid=POD_UID,
            dependencies=orch_main._unit_claim_bundle_dependencies(),
        )
    assert exc.value.status_code == 409
    # Generic reason — must not leak which fail-closed rule refused.
    assert "grant" not in str(exc.value.detail).lower()
    assert "datasource" not in str(exc.value.detail).lower()


@pytest.mark.asyncio
async def test_internal_auth_failure_is_401_before_any_lookup(monkeypatch):
    """With the REAL require_internal and no/wrong X-Internal-Key the endpoint
    401s before touching the queue or the thread."""
    from orchestrator import main as orch_main

    db = FakeDB(run_queue_row=dict(LEASED_ROW), thread=_thread())
    monkeypatch.setattr(orch_main, "postgres_db", db)

    # R1.B06: the transport guard is the router's, so this drives the router.
    # The point of the case is unchanged and still worth holding: the 401 must
    # land before the service is entered at all, which is why the db assertion
    # below is the real assertion.
    from orchestrator.routers import unit_claim as unit_claim_router

    request = MagicMock()
    request.headers = {"X-Internal-Key": ""}
    request.app.state.unit_claim_bundle_dependencies_factory = (
        orch_main._unit_claim_bundle_dependencies
    )
    with pytest.raises(HTTPException) as exc:
        await unit_claim_router.internal_unit_claim_bundle(
            UNIT_ID, request, 7, POD_NAME, POD_UID
        )
    assert exc.value.status_code == 401
    db.conn.fetchrow.assert_not_awaited()


class ProtocolRecoveryStore:
    """Persistence boundary double: acceptance survives each HTTP request."""

    def __init__(self, db):
        self.db = db
        self.receipt = None
        self.requests = {}
        self.authorized = False

    async def _accepted_request(self, conn, *, job_id, request_id, intent_digest):
        prior = self.requests.get(request_id)
        if prior and prior[0] != intent_digest:
            raise RuntimeError(
                "workspace recovery request ID reused with different intent"
            )
        return prior[1] if prior else None

    async def get_attempt_disposition(self, conn, *, job_id, lease_token):
        from shared.workspace_recovery import RecoveryAttemptDisposition

        if lease_token != 7:
            return None
        result = None
        if self.receipt:
            result = {
                "code": self.receipt.code.value,
                **self.receipt.as_error_detail()["recovery"],
            }
        return RecoveryAttemptDisposition(
            job_id,
            7,
            self.authorized,
            None,
            result,
            self.receipt.operation_id if self.receipt else None,
            False,
        )

    async def admit_hold(self, **kwargs):
        from shared.workspace_recovery import (
            WorkspaceRecoveryDisposition,
            WorkspaceRecoveryCode,
        )

        assert kwargs["accepted_lease_token"] == 7
        assert kwargs["owner_id"] == UUID(UNIT_ID)
        assert kwargs["prior_launcher_uid"] == UUID(WORKSPACE_RUNTIME)
        factory = WorkspaceRecoveryDisposition.hold_committed
        code = kwargs["code"]
        if any(
            kwargs[key] is None
            for key in (
                "prior_vmi_uid",
                "prior_launcher_uid",
                "vm_uid",
                "root_pvc_uid",
                "provision_generation",
                "namespace",
            )
        ):
            factory = WorkspaceRecoveryDisposition.paused_attention
            code = WorkspaceRecoveryCode.IDENTITY_CONFLICT
        self.receipt = factory(
            operation_id=UUID("cccccccc-cccc-4ccc-8ccc-cccccccccccc"),
            accepted_lease_token=7,
            hold_lease_token=8,
            code=code,
        )
        self.requests[kwargs["request_id"]] = (kwargs["intent_digest"], self.receipt)
        self.db._row.update(state="parked", lease_token=8)
        return self.receipt

    async def record_bundle_authorized(self, conn, **kwargs):
        assert kwargs["job_id"] == UUID(UNIT_ID)
        assert kwargs["lease_token"] == 7
        assert kwargs["authority_digest"]
        self.authorized = True
        return True


def recovery_protocol_app(monkeypatch, *, ready=False, complete_identity=True):
    from fastapi import FastAPI
    from orchestrator import main as orch_main
    from orchestrator.routers.unit_claim import router

    monkeypatch.setenv("VM_WORKSPACE_RECOVERY_ENABLED", "true")
    monkeypatch.setenv("VM_MODE", "same-cluster")
    vm = _ready_worker_vm(
        ssh_ready_source="provisioner_probe" if ready else None,
        vm_uid="33333333-3333-4333-8333-333333333333",
        vmi_uid="44444444-4444-4444-8444-444444444444" if complete_identity else None,
        rootdisk_pvc_uid="55555555-5555-4555-8555-555555555555",
        namespace="agent-vms",
        cluster_name="local",
    )
    job = {
        "id": UNIT_ID,
        "execution_lane": "stateless",
        "config_override": {"workspace": {"backend": "vm"}},
        "context": _worker_vm_context(vm),
    }
    db = FakeDB(
        run_queue_row=dict(LEASED_ROW, unit_kind="worker_batch"), thread=None, job=job
    )
    store = ProtocolRecoveryStore(db)
    monkeypatch.setattr(orch_main, "postgres_db", db)
    monkeypatch.setattr(orch_main, "require_internal", AsyncMock())
    monkeypatch.setattr(
        orch_main, "VMWorkspaceRecoveryStore", lambda db: store, raising=False
    )
    monkeypatch.setattr(
        job_workspace_authority,
        "resolve_subjob_inherited_workspace",
        AsyncMock(return_value=("proceed", None)),
    )
    monkeypatch.setattr(
        job_start_bundle,
        "build_job_start_request",
        AsyncMock(
            return_value=orch_main.JobStartRequest(job_id=UNIT_ID, description="work")
        ),
    )
    _patch_vm_worker_attestation(monkeypatch, orch_main)
    claimant = _patch_worker_claimant(monkeypatch, orch_main)
    db.claimant_mock = claimant
    app = FastAPI()
    app.include_router(router)
    app.state.unit_claim_bundle_dependencies_factory = (
        orch_main._unit_claim_bundle_dependencies
    )
    return app, db, store


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "complete_identity,action", [(True, "hold_committed"), (False, "paused_attention")]
)
async def test_bundle_runtime_refusal_commits_and_replays_hold(
    monkeypatch, complete_identity, action
):
    import httpx

    app, db, store = recovery_protocol_app(
        monkeypatch, complete_identity=complete_identity
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        params = {"lease_token": 7, "pod_name": POD_NAME, "pod_uid": POD_UID}
        first = await client.get(
            f"/internal/units/{UNIT_ID}/claim-bundle", params=params
        )
        assert first.status_code == 409
        assert first.json()["recovery"]["action"] == action
        assert db._row["lease_token"] == 8
        # Discarding the first response models delivery loss; stale replay must
        # recover the committed receipt before ordinary live-lease rejection.
        replay = await client.get(
            f"/internal/units/{UNIT_ID}/workspace-recovery-disposition",
            params={"lease_token": 7},
        )
        assert replay.status_code == 200
        assert replay.json() == first.json()
        retry = await client.get(
            f"/internal/units/{UNIT_ID}/claim-bundle", params=params
        )
        assert retry.status_code == 409
        assert retry.json() == first.json()
        assert "job" not in retry.json()
        wrong = await client.get(
            f"/internal/units/{UNIT_ID}/workspace-recovery-disposition",
            params={"lease_token": 6},
        )
        assert wrong.status_code == 404


@pytest.mark.asyncio
async def test_hold_post_response_loss_replays_and_rejects_changed_intent(monkeypatch):
    import httpx

    app, db, store = recovery_protocol_app(monkeypatch)
    report = {
        "lease_token": 7,
        "pod_name": POD_NAME,
        "pod_uid": POD_UID,
        "code": "workspace_transport_unavailable",
        "request_id": "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
    }
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        url = f"/internal/units/{UNIT_ID}/workspace-recovery"
        first = await client.post(url, json=report)
        assert first.status_code == 200
        replay = await client.post(url, json=report)
        assert replay.status_code == 200
        assert replay.json() == first.json()
        changed = await client.post(
            url, json={**report, "code": "workspace_runtime_not_ready"}
        )
        assert changed.status_code == 409
        assert "recovery" not in changed.json()
        forged = await client.post(
            url, json={**report, "stop_proof": {"stopped": True}}
        )
        assert forged.status_code == 422


@pytest.mark.asyncio
async def test_accepted_replay_survives_pod_disappearance_without_second_hold(
    monkeypatch,
):
    """Accepted exact replay returns its disposition after rotation/Pod loss.

    The fresh-report path validates live Pod authority, but matching accepted
    replay is idempotent and must not demand a current lease or live Pod:
    admission already rotated the token and the original response may be lost.
    A conflicting replay or a stale new request must still be refused without
    a second hold.
    """
    import httpx
    from fastapi import HTTPException as _HTTPException

    app, db, store = recovery_protocol_app(monkeypatch)
    report = {
        "lease_token": 7,
        "pod_name": POD_NAME,
        "pod_uid": POD_UID,
        "code": "workspace_transport_unavailable",
        "request_id": "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
    }
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        url = f"/internal/units/{UNIT_ID}/workspace-recovery"
        first = await client.post(url, json=report)
        assert first.status_code == 200
        holds_before = len(
            [key for key in store.requests if store.requests[key][1] is not None]
        )
        # The Pod disappears (and the lease rotated to the hold token) after
        # admission. Exact replay must still return the stored disposition.
        db.claimant_mock.side_effect = _HTTPException(403, "Lease validation failed")
        replay = await client.post(url, json=report)
        assert replay.status_code == 200
        assert replay.json() == first.json()
        assert len(store.requests) == holds_before
        # A stale new recovery request for the rotated lease must not admit a
        # second hold for a stale claimant.
        stale = await client.post(
            url,
            json={**report, "request_id": "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"},
        )
        assert stale.status_code in {403, 409}
        assert len(store.requests) == holds_before


@pytest.mark.asyncio
async def test_fresh_recovery_report_without_live_claimant_refused(monkeypatch):
    import httpx
    from fastapi import HTTPException as _HTTPException

    app, db, store = recovery_protocol_app(monkeypatch)
    db.claimant_mock.side_effect = _HTTPException(403, "Lease validation failed")
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.post(
            f"/internal/units/{UNIT_ID}/workspace-recovery",
            json={
                "lease_token": 7,
                "pod_name": POD_NAME,
                "pod_uid": POD_UID,
                "code": "workspace_transport_unavailable",
                "request_id": "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
            },
        )
    assert response.status_code == 403
    assert store.receipt is None


@pytest.mark.asyncio
async def test_bundle_records_authority_before_credentials_and_stale_retry_never_replays_them(
    monkeypatch,
):
    import httpx

    app, db, store = recovery_protocol_app(monkeypatch, ready=True)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        params = {"lease_token": 7, "pod_name": POD_NAME, "pod_uid": POD_UID}
        first = await client.get(
            f"/internal/units/{UNIT_ID}/claim-bundle", params=params
        )
        assert first.status_code == 200
        assert store.authorized
        db._row.update(lease_token=8)
        stale = await client.get(
            f"/internal/units/{UNIT_ID}/claim-bundle", params=params
        )
        assert stale.status_code == 403
        assert "job" not in stale.json()


@pytest.mark.asyncio
async def test_recovery_routes_require_internal_auth(monkeypatch):
    import httpx
    from orchestrator import main as orch_main

    app, db, store = recovery_protocol_app(monkeypatch)
    monkeypatch.setattr(
        orch_main,
        "require_internal",
        AsyncMock(side_effect=HTTPException(401, "Unauthorized")),
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get(
            f"/internal/units/{UNIT_ID}/workspace-recovery-disposition",
            params={"lease_token": 7},
        )
        assert response.status_code == 401
        response = await client.post(
            f"/internal/units/{UNIT_ID}/workspace-recovery",
            json={
                "lease_token": 7,
                "pod_name": POD_NAME,
                "pod_uid": POD_UID,
                "code": "workspace_transport_unavailable",
                "request_id": "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
            },
        )
        assert response.status_code == 401
        assert store.receipt is None


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["unknown", "untyped_409", "untyped_authority"])
async def test_vm_unknown_attestation_refusals_remain_generic(monkeypatch, failure):
    import httpx
    from orchestrator import main as orch_main
    from orchestrator.services.container_provisioner import (
        WorkspaceRuntimeAuthorityError,
    )

    app, db, store = recovery_protocol_app(monkeypatch, ready=True)
    error = {
        "unknown": RuntimeError("unexpected bug"),
        "untyped_409": HTTPException(409, "unknown refusal"),
        "untyped_authority": WorkspaceRuntimeAuthorityError("unavailable"),
    }[failure]
    monkeypatch.setattr(
        orch_main.vm_provisioner,
        "attest_workspace_runtime",
        AsyncMock(side_effect=error),
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get(
            f"/internal/units/{UNIT_ID}/claim-bundle",
            params={"lease_token": 7, "pod_name": POD_NAME, "pod_uid": POD_UID},
        )
    assert response.status_code == 409
    assert "recovery" not in response.json()
    assert "code" not in response.json()
    assert store.receipt is None
    assert not store.authorized


@pytest.mark.asyncio
async def test_untyped_authority_boundary_409_does_not_admit_recovery(monkeypatch):
    import httpx

    app, db, store = recovery_protocol_app(monkeypatch, ready=True)
    monkeypatch.setattr(
        job_workspace_authority,
        "attest_stateless_worker_vm_workspace",
        AsyncMock(side_effect=HTTPException(409, "generic refusal")),
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get(
            f"/internal/units/{UNIT_ID}/claim-bundle",
            params={"lease_token": 7, "pod_name": POD_NAME, "pod_uid": POD_UID},
        )
    assert response.status_code == 409
    assert "recovery" not in response.json()
    assert store.receipt is None


@pytest.mark.asyncio
async def test_disabled_admission_still_records_bundle_authorization(monkeypatch):
    import httpx

    app, db, store = recovery_protocol_app(monkeypatch, ready=True)
    monkeypatch.setenv("VM_WORKSPACE_RECOVERY_ENABLED", "false")
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get(
            f"/internal/units/{UNIT_ID}/claim-bundle",
            params={"lease_token": 7, "pod_name": POD_NAME, "pod_uid": POD_UID},
        )
    assert response.status_code == 200
    assert store.authorized


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "observation,code",
    [
        ({"ready": False}, "workspace_runtime_not_ready"),
        (
            {"active_pod_uid": "66666666-6666-4666-8666-666666666666"},
            "workspace_replacement_observed",
        ),
    ],
)
async def test_explicit_vm_attestation_condition_commits_typed_hold(
    monkeypatch, observation, code
):
    import httpx
    from orchestrator import main as orch_main
    from orchestrator.services.vm_provisioner import VMProvisioner

    app, db, store = recovery_protocol_app(monkeypatch, ready=True)
    db._job["context"]["vm"]["ssh_registration_id"] = "registration-1"
    vm = db._job["context"]["vm"]
    provisioner = VMProvisioner()
    provisioner._controller_url = "http://vm-controller:8080"
    provisioner._http_client = MagicMock()
    provisioner._lifecycle_hmac_secret = b"test-attestation-secret-at-least-32-bytes"
    provisioner._db = db
    provisioner._query_http = AsyncMock(
        return_value={
            "_identity_authenticated": True,
            "provision_generation": vm["provision_generation"],
            "vm_uid": vm["vm_uid"],
            "active_pod_uid": vm["active_pod_uid"],
            "pod_ip": vm["pod_ip"],
            "ready": True,
            **observation,
        }
    )
    monkeypatch.setattr(orch_main, "vm_provisioner", provisioner)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get(
            f"/internal/units/{UNIT_ID}/claim-bundle",
            params={"lease_token": 7, "pod_name": POD_NAME, "pod_uid": POD_UID},
        )
    assert response.status_code == 409
    assert response.json()["recovery"]["action"] == "hold_committed"
    assert response.json()["code"] == code
    assert not store.authorized


@pytest.mark.asyncio
@pytest.mark.parametrize("refusal", ["pod", "authorization", "assembly"])
async def test_bundle_fails_closed_without_recovering_unrelated_refusals(
    monkeypatch, refusal
):
    import httpx

    from fastapi import HTTPException as _HTTPException

    app, db, store = recovery_protocol_app(monkeypatch, ready=True)
    if refusal == "pod":
        db.claimant_mock.side_effect = _HTTPException(403, "Lease validation failed")
    elif refusal == "authorization":
        store.record_bundle_authorized = AsyncMock(return_value=False)
    else:
        monkeypatch.setattr(
            job_start_bundle, "build_job_start_request", AsyncMock(return_value=None)
        )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get(
            f"/internal/units/{UNIT_ID}/claim-bundle",
            params={"lease_token": 7, "pod_name": POD_NAME, "pod_uid": POD_UID},
        )
    assert response.status_code == (409 if refusal == "assembly" else 403)
    assert "recovery" not in response.json()
    assert "job" not in response.json()
    assert store.receipt is None


@pytest.mark.asyncio
async def test_legacy_vm_missing_root_disk_identity_still_gets_attention_hold(
    monkeypatch,
):
    import httpx

    app, db, store = recovery_protocol_app(monkeypatch)
    db._job["context"]["vm"].pop("rootdisk_pvc_uid")
    db._job["context"]["vm"].pop("cluster_name")
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get(
            f"/internal/units/{UNIT_ID}/claim-bundle",
            params={"lease_token": 7, "pod_name": POD_NAME, "pod_uid": POD_UID},
        )
    assert response.status_code == 409
    assert response.json()["recovery"]["action"] == "paused_attention"


@pytest.mark.asyncio
async def test_disabling_admission_preserves_committed_receipt_replay(monkeypatch):
    import httpx

    app, db, store = recovery_protocol_app(monkeypatch)
    params = {"lease_token": 7, "pod_name": POD_NAME, "pod_uid": POD_UID}
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        first = await client.get(
            f"/internal/units/{UNIT_ID}/claim-bundle", params=params
        )
        assert first.status_code == 409
        monkeypatch.setenv("VM_WORKSPACE_RECOVERY_ENABLED", "false")
        replay = await client.get(
            f"/internal/units/{UNIT_ID}/workspace-recovery-disposition",
            params={"lease_token": 7},
        )
        assert replay.status_code == 200
        assert replay.json() == first.json()
        retry = await client.get(
            f"/internal/units/{UNIT_ID}/claim-bundle", params=params
        )
        assert retry.status_code == 409
        assert retry.json() == first.json()


@pytest.mark.asyncio
async def test_bundle_malformed_worker_uid_is_generic_lease_refusal(monkeypatch):
    import httpx

    app, db, store = recovery_protocol_app(monkeypatch)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get(
            f"/internal/units/{UNIT_ID}/claim-bundle",
            params={"lease_token": 7, "pod_name": POD_NAME, "pod_uid": "not-a-uid"},
        )
    assert response.status_code == 403
    assert "recovery" not in response.json()


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["inherited", "replacement"])
async def test_vm_runtime_refusals_during_assembly_commit_hold(monkeypatch, stage):
    import httpx
    from orchestrator import main as orch_main

    app, db, store = recovery_protocol_app(monkeypatch, ready=True)
    if stage == "inherited":
        monkeypatch.setattr(
            job_workspace_authority,
            "resolve_subjob_inherited_workspace",
            AsyncMock(return_value=("wait", None)),
        )
    else:
        _patch_vm_worker_attestation(
            monkeypatch,
            orch_main,
            _vm_worker_attestation(orch_main),
            _vm_worker_attestation(
                orch_main, runtime_incarnation="66666666-6666-4666-8666-666666666666"
            ),
        )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get(
            f"/internal/units/{UNIT_ID}/claim-bundle",
            params={"lease_token": 7, "pod_name": POD_NAME, "pod_uid": POD_UID},
        )
    assert response.status_code == 409
    assert response.json()["recovery"]["action"] == "hold_committed"
    assert not store.authorized
