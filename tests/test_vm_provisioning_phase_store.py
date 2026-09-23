"""Real PostgreSQL observation ordering, identity and control races."""

import asyncio
import json
from uuid import UUID, uuid4, uuid5

import pytest

from orchestrator.services.vm_provisioning_phases import VMProvisioningPhaseStore
from tests.test_non_pinned_workspace_lifecycle_real_postgres import (
    _schema_applied,  # noqa: F401
    db as _postgres_db_fixture,
    pg_dsn,  # noqa: F401
)
from tests.test_vm_provisioning_phases import evidence, running

db = _postgres_db_fixture


async def seed(db, *, vm_fields=None, status="created"):
    job, generation = uuid4(), str(uuid4())
    vm = {"provision_generation": generation, "status": "starting", **(vm_fields or {})}
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO jobs(id,description,status,context) VALUES($1,'phase test',$2,$3::jsonb)",
            job,
            status,
            json.dumps({"vm": vm}),
        )
    return str(job), generation


def status(job, generation, *, boot=False, **changes):
    # Each owner has its own disk. Reusing the pure-policy fixture's constant
    # PVC across jobs makes a prior cleanup permit block unrelated race tests.
    changes.setdefault("rootdisk_pvc_uid", str(uuid5(UUID(job), "rootdisk")))
    observation = (running if boot else evidence)(
        owner_id=job, provision_generation=generation, **changes
    )
    return {
        "provision_generation": generation,
        "vm_name": f"agent-vm-{job}",
        "namespace": "srw",
        "vm_uid": observation["vm_uid"],
        "rootdisk_pvc_uid": observation["rootdisk_pvc_uid"],
        "provisioning": observation,
    }


async def vm_context(db, job):
    async with db.acquire() as conn:
        value = await conn.fetchval(
            "SELECT context->'vm' FROM jobs WHERE id=$1", UUID(job)
        )
    return json.loads(value) if isinstance(value, str) else value


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "winner", ["attention", "ready", "initializing", "revision", "cancelled"]
)
async def test_newer_observation_or_control_refuses_stale_boot_cleanup_without_reservation(
    db, winner
):
    from orchestrator.services.vm_provisioner import VMTeardownIdentity
    from shared.vm_provisioning_phases import observe_provisioning

    job, generation = await seed(db)
    observation = status(job, generation, boot=True)
    stale = {
        "status": "starting",
        "provisioning_revision": 1,
        **{
            key: observation[key]
            for key in (
                "vm_uid",
                "rootdisk_pvc_uid",
                "namespace",
                "provision_generation",
            )
        },
        "provisioning": observe_provisioning(
            None, observation["provisioning"], now=100
        ),
    }
    await db.merge_vm_context_if_provision_generation(job, generation, stale)
    update = {
        "attention": {"provisioning_attention_reason": "vm_phase_identity_conflict"},
        "ready": {"status": "ready"},
        "initializing": {"initialization_started_at": 200},
        "revision": {"provisioning_revision": 2},
        "cancelled": {},
    }[winner]
    await db.merge_vm_context_if_provision_generation(job, generation, update)
    if winner == "cancelled":
        async with db.acquire() as conn:
            await conn.execute(
                "UPDATE jobs SET status='cancelled' WHERE id=$1", UUID(job)
            )
    result = await VMProvisioningPhaseStore(db).admit_boot_cleanup(
        job,
        stale,
        identity=VMTeardownIdentity(
            generation, stale["vm_uid"], stale["rootdisk_pvc_uid"]
        ),
        boot_timeout_s=600,
        rootdisk_stall_timeout_s=2700,
    )
    assert result is None
    async with db.acquire() as conn:
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM vm_workspace_cleanup_admissions WHERE owner_id=$1",
                UUID(job),
            )
            == 0
        )
    assert (await vm_context(db, job)).get("retirement_cleanup_pending") is not True


@pytest.mark.asyncio
async def test_boot_cleanup_winner_blocks_late_phase_and_ready_promotion(db):
    from orchestrator.services.vm_provisioner import VMTeardownIdentity
    from shared.vm_provisioning_phases import observe_provisioning

    job, generation = await seed(db)
    observation = status(job, generation, boot=True)
    stale = {
        "status": "starting",
        "provisioning_revision": 1,
        **{
            key: observation[key]
            for key in (
                "vm_uid",
                "rootdisk_pvc_uid",
                "namespace",
                "provision_generation",
            )
        },
        "provisioning": observe_provisioning(
            None, observation["provisioning"], now=100
        ),
    }
    await db.merge_vm_context_if_provision_generation(job, generation, stale)
    await db.merge_vm_context_if_provision_generation(
        job, generation, {"ssh_registration_id": "registration"}
    )
    store = VMProvisioningPhaseStore(db)
    token = await store.capture(job, generation)
    permit = await store.admit_boot_cleanup(
        job,
        stale,
        identity=VMTeardownIdentity(
            generation, stale["vm_uid"], stale["rootdisk_pvc_uid"]
        ),
        boot_timeout_s=600,
        rootdisk_stall_timeout_s=2700,
    )
    assert permit.allowed
    assert (await vm_context(db, job))["retirement_cleanup_pending"] is True
    assert await store.apply_status(token, observation) == "held"
    assert not await db.merge_vm_context_if_provision_generation(
        job, generation, {"status": "ready"}
    )
    assert not await db.merge_vm_context_if_provision_generation(
        job, generation, {"initialization_started_at": 300}
    )
    assert not await db.merge_vm_context_if_current(
        job, "registration", {"status": "ready"}
    )
    assert not await db.merge_vm_context_if_current(
        job, "registration", {"initialization_started_at": 300}
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "update",
    [
        {"status": "ready"},
        {"initialization_started_at": 200},
        {"provisioning_attention_reason": "vm_phase_identity_conflict"},
    ],
)
async def test_boot_cleanup_rechecks_winner_committed_during_job_lock_wait(db, update):
    from orchestrator.services.vm_provisioner import VMTeardownIdentity
    from shared.vm_provisioning_phases import observe_provisioning

    job, generation = await seed(db)
    observation = status(job, generation, boot=True)
    stale = {
        "status": "starting",
        "provisioning_revision": 1,
        **{
            key: observation[key]
            for key in (
                "vm_uid",
                "rootdisk_pvc_uid",
                "namespace",
                "provision_generation",
            )
        },
        "provisioning": observe_provisioning(
            None, observation["provisioning"], now=100
        ),
    }
    await db.merge_vm_context_if_provision_generation(job, generation, stale)
    async with db.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                "UPDATE jobs SET context=jsonb_set(context,'{vm}',context->'vm' || $2::jsonb) WHERE id=$1",
                UUID(job),
                json.dumps(update),
            )
            pending = asyncio.create_task(
                VMProvisioningPhaseStore(db).admit_boot_cleanup(
                    job,
                    stale,
                    identity=VMTeardownIdentity(
                        generation, stale["vm_uid"], stale["rootdisk_pvc_uid"]
                    ),
                    boot_timeout_s=600,
                    rootdisk_stall_timeout_s=2700,
                )
            )
            await asyncio.sleep(0.2)
            assert not pending.done()
    assert await asyncio.wait_for(pending, 5) is None
    assert (await vm_context(db, job)).get("retirement_cleanup_pending") is not True


@pytest.mark.asyncio
async def test_two_observers_commit_once_without_regressing_progress(db):
    job, generation = await seed(db)
    store = VMProvisioningPhaseStore(db)
    first, second = await asyncio.gather(
        store.capture(job, generation), store.capture(job, generation)
    )
    assert (
        await store.apply_status(second, status(job, generation, disk_progress=80))
        == "observed"
    )
    assert (
        await store.apply_status(first, status(job, generation, disk_progress=10))
        == "stale"
    )
    stored = await vm_context(db, job)
    assert stored["provisioning"]["disk_progress_high_water"] == 80
    assert stored["provisioning_revision"] == 1
    assert stored["vm_uid"] == evidence()["vm_uid"]


@pytest.mark.asyncio
async def test_restart_preserves_first_boot_start_and_duplicate_poll_is_not_progress(
    db,
):
    job, generation = await seed(db)
    store = VMProvisioningPhaseStore(db)
    await store.apply_status(
        await store.capture(job, generation), status(job, generation, boot=True)
    )
    first = await vm_context(db, job)
    restarted = VMProvisioningPhaseStore(db)
    await restarted.apply_status(
        await restarted.capture(job, generation), status(job, generation, boot=True)
    )
    second = await vm_context(db, job)
    assert (
        second["provisioning"]["first_guest_started_at"]
        == first["provisioning"]["first_guest_started_at"]
    )
    assert (
        second["provisioning"]["last_real_progress_at"]
        == first["provisioning"]["last_real_progress_at"]
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal", ["cancelled", "completed", "failed"])
async def test_terminal_control_wins_against_inflight_observation(db, terminal):
    job, generation = await seed(db)
    store = VMProvisioningPhaseStore(db)
    token = await store.capture(job, generation)
    async with db.acquire() as conn:
        await conn.execute("UPDATE jobs SET status=$2 WHERE id=$1", UUID(job), terminal)
    assert await store.apply_status(token, status(job, generation, boot=True)) == "held"
    assert "provisioning" not in await vm_context(db, job)
    assert await store.capture(job, generation) is None


@pytest.mark.asyncio
async def test_active_or_malformed_control_claim_blocks_phase_and_identity(db):
    job, generation = await seed(db)
    store = VMProvisioningPhaseStore(db)
    token = await store.capture(job, generation)
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET context=context || '{\"_completion_control_claim\":true}'::jsonb WHERE id=$1",
            UUID(job),
        )
    assert await store.apply_status(token, status(job, generation)) == "held"
    assert "vm_uid" not in await vm_context(db, job)


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["vm_uid", "rootdisk_pvc_uid", "namespace"])
async def test_bound_identity_change_preserves_old_identity_and_records_attention(
    db, field
):
    bound = {
        "vm_uid": evidence()["vm_uid"],
        "rootdisk_pvc_uid": evidence()["rootdisk_pvc_uid"],
        "namespace": "srw",
    }
    job, generation = await seed(db, vm_fields=bound)
    store = VMProvisioningPhaseStore(db)
    reply = status(job, generation)
    reply[field] = "other" if field == "namespace" else str(uuid4())
    reply["provisioning"][field] = reply[field]
    assert (
        await store.apply_status(await store.capture(job, generation), reply)
        == "conflict"
    )
    stored = await vm_context(db, job)
    assert stored[field] == bound[field]
    assert stored["provisioning_attention_reason"] == "vm_phase_identity_conflict"


@pytest.mark.asyncio
async def test_nested_and_outer_identity_must_agree_even_before_first_binding(db):
    job, generation = await seed(db)
    store = VMProvisioningPhaseStore(db)
    reply = status(job, generation)
    reply["provisioning"]["vm_uid"] = str(uuid4())
    assert (
        await store.apply_status(await store.capture(job, generation), reply)
        == "conflict"
    )
    assert "vm_uid" not in await vm_context(db, job)


@pytest.mark.asyncio
async def test_first_disk_binding_checks_frozen_storage_request(db):
    job, generation = await seed(
        db,
        vm_fields={
            "creation_request": {
                "request": {"workspace_storage": {"pvc_uid": str(uuid4())}}
            },
        },
    )
    store = VMProvisioningPhaseStore(db)
    assert (
        await store.apply_status(
            await store.capture(job, generation), status(job, generation)
        )
        == "conflict"
    )


@pytest.mark.asyncio
async def test_unknown_observation_retains_existing_clock_and_bound_disk(db):
    job, generation = await seed(db)
    store = VMProvisioningPhaseStore(db)
    await store.apply_status(
        await store.capture(job, generation), status(job, generation, boot=True)
    )
    first = await vm_context(db, job)
    unknown = status(job, generation)
    unknown.pop("provisioning")
    assert (
        await store.apply_status(await store.capture(job, generation), unknown)
        == "unproven"
    )
    second = await vm_context(db, job)
    assert second["provisioning"] == first["provisioning"]
    assert second["rootdisk_pvc_uid"] == first["rootdisk_pvc_uid"]
    assert second["provisioning_attention_reason"] == "vm_phase_unproven"


@pytest.mark.asyncio
async def test_late_token_cannot_overwrite_attention_with_old_good_evidence(db):
    job, generation = await seed(db)
    store = VMProvisioningPhaseStore(db)
    early = await store.capture(job, generation)
    later = await store.capture(job, generation)
    unknown = status(job, generation)
    unknown.pop("provisioning")
    await store.apply_status(later, unknown)
    assert (
        await store.apply_status(early, status(job, generation, boot=True)) == "stale"
    )
    assert "provisioning" not in await vm_context(db, job)


@pytest.mark.asyncio
async def test_exact_new_progress_clears_only_phase_attention(db):
    job, generation = await seed(
        db,
        vm_fields={
            "provisioning_attention_reason": "vm_phase_unproven",
            "retirement_last_result": "other-history",
        },
    )
    store = VMProvisioningPhaseStore(db)
    assert (
        await store.apply_status(
            await store.capture(job, generation), status(job, generation)
        )
        == "observed"
    )
    stored = await vm_context(db, job)
    assert stored["provisioning_attention_reason"] is None
    assert stored["retirement_last_result"] == "other-history"


@pytest.mark.asyncio
async def test_cleanup_pending_and_stale_generation_cannot_capture(db):
    job, generation = await seed(db, vm_fields={"retirement_cleanup_pending": True})
    store = VMProvisioningPhaseStore(db)
    assert await store.capture(job, generation) is None
    assert await store.capture(job, str(uuid4())) is None


async def provisioner_with_response(db, monkeypatch, response):
    import httpx
    from orchestrator.services.vm_provisioner import VMProvisioner
    from shared.vm_lifecycle_auth import sign_payload

    secret = b"phase-store-transport-test-secret-at-least-32-bytes"
    monkeypatch.setenv("VM_MODE", "same-cluster")
    monkeypatch.setenv("VM_CONTROLLER_URL", "http://phase-test")
    provisioner = VMProvisioner()
    provisioner.connect(db)
    await provisioner._http_client.aclose()
    provisioner._lifecycle_hmac_secret = secret

    async def handler(request):
        body = await response(request)
        return httpx.Response(
            200,
            json=sign_payload(
                body,
                direction="response",
                operation="status",
                secret=secret,
                correlation_id=request.url.params["lifecycle_auth_request_id"],
            ),
        )

    provisioner._http_client = httpx.AsyncClient(
        base_url="http://phase-test", transport=httpx.MockTransport(handler)
    )
    return provisioner


@pytest.mark.asyncio
async def test_authenticated_transport_persists_phase_and_identity_in_one_guard(
    db, monkeypatch
):
    job, generation = await seed(db)

    async def reply(_request):
        return status(job, generation, boot=True)

    provisioner = await provisioner_with_response(db, monkeypatch, reply)
    try:
        assert await provisioner.query_status(job) is not None
    finally:
        await provisioner._http_client.aclose()
    stored = await vm_context(db, job)
    assert stored["provisioning"]["phase"] == "boot"
    assert stored["identity_provision_generation"] == generation


@pytest.mark.asyncio
async def test_authenticated_first_vmi_binding_reaches_exact_runtime_attestation(
    db, monkeypatch
):
    """Catch a signed VMI that reaches phase history but not runtime authority."""
    job, generation = await seed(db)
    vmi_uid, launcher_uid, registration = (str(uuid4()) for _ in range(3))
    fingerprint = "SHA256:" + "A" * 43
    observed = status(job, generation, boot=True, vmi_uid=vmi_uid)
    observed.update(
        vmi_uid=vmi_uid,
        ready=True,
        active_pod_uid=launcher_uid,
        pod_ip="10.42.0.90",
        ssh_host_key_fingerprint=fingerprint,
        credential_runtime_started=True,
    )

    async def reply(_request):
        return observed

    provisioner = await provisioner_with_response(db, monkeypatch, reply)
    try:
        assert await provisioner.query_status(job) is not None
        stored = await vm_context(db, job)
        assert stored["provisioning"]["identity"]["vmi_uid"] == vmi_uid
        assert stored["vmi_uid"] == vmi_uid
        assert await db.merge_vm_context_if_provision_generation(
            job,
            generation,
            {
                "active_pod_uid": launcher_uid,
                "ssh_registration_id": registration,
                "ssh_host": "10.42.0.90",
                "pod_ip": "10.42.0.90",
                "ssh_port": 22,
            },
        )
        attested = await provisioner.attest_workspace_runtime(job)
        assert attested.vmi_uid == vmi_uid
        assert attested.launcher_pod_uid == launcher_uid
    finally:
        await provisioner._http_client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case", ["outer_nested", "previous_bound", "identity_updates", "malformed"]
)
async def test_phase_store_refuses_unmatched_vmi_identity_without_rebinding(db, case):
    """A phase reply cannot bind a VMI distinct from its phase/old identity."""
    previous = str(uuid4()) if case == "previous_bound" else None
    job, generation = await seed(
        db, vm_fields={"vmi_uid": previous} if previous else None
    )
    reply = status(job, generation, boot=True, vmi_uid=str(uuid4()))
    reply["vmi_uid"] = reply["provisioning"]["vmi_uid"]
    updates = {"vmi_uid": reply["vmi_uid"]}
    if case == "outer_nested":
        reply["vmi_uid"] = str(uuid4())
        updates["vmi_uid"] = reply["vmi_uid"]
    elif case == "identity_updates":
        updates["vmi_uid"] = str(uuid4())
    elif case == "malformed":
        reply["vmi_uid"] = "malformed-vmi"
        updates = {}
    result = await VMProvisioningPhaseStore(db).apply_status(
        await VMProvisioningPhaseStore(db).capture(job, generation),
        reply,
        identity_updates=updates,
    )
    assert result in {"conflict", "held"}
    stored = await vm_context(db, job)
    assert stored.get("vmi_uid") == previous


@pytest.mark.asyncio
async def test_phase_store_pending_or_missing_vmi_never_clears_known_binding(db):
    job, generation = await seed(db)
    store = VMProvisioningPhaseStore(db)
    pending = status(job, generation)
    pending["vmi_uid"] = None
    assert (
        await store.apply_status(await store.capture(job, generation), pending)
        == "observed"
    )
    assert (await vm_context(db, job)).get("vmi_uid") is None
    bound = status(job, generation, boot=True, vmi_uid=str(uuid4()))
    bound["vmi_uid"] = bound["provisioning"]["vmi_uid"]
    assert (
        await store.apply_status(await store.capture(job, generation), bound)
        == "observed"
    )
    identity = (await vm_context(db, job))["vmi_uid"]
    assert (
        await store.apply_status(await store.capture(job, generation), bound)
        == "observed"
    )
    missing = dict(bound)
    missing.pop("vmi_uid")
    assert (
        await store.apply_status(await store.capture(job, generation), missing)
        == "observed"
    )
    assert (await vm_context(db, job))["vmi_uid"] == identity


@pytest.mark.asyncio
async def test_unproven_legacy_phase_reply_keeps_matching_bound_vmi(db):
    """Missing phase evidence remains unproven without rewriting old identity."""
    vmi_uid = str(uuid4())
    job, generation = await seed(db, vm_fields={"vmi_uid": vmi_uid})
    reply = status(job, generation, boot=True, vmi_uid=vmi_uid)
    reply["vmi_uid"] = vmi_uid
    reply.pop("provisioning")
    store = VMProvisioningPhaseStore(db)
    assert (
        await store.apply_status(await store.capture(job, generation), reply)
        == "unproven"
    )
    stored = await vm_context(db, job)
    assert stored["vmi_uid"] == vmi_uid
    assert stored["provisioning_attention_reason"] == "vm_phase_unproven"


@pytest.mark.asyncio
async def test_authenticated_vmi_binding_allows_real_profile_ready_promotion(
    db, monkeypatch
):
    """Use the real signed status, PG phase store and attestation before Ready."""
    from unittest.mock import AsyncMock

    from orchestrator.services.vm_readiness import VMReadinessService
    from shared.vm_network_profile import NETWORK_PROFILE

    job, generation = await seed(
        db,
        vm_fields={
            "creation_preflight": {"request": {"network_profile": NETWORK_PROFILE}}
        },
    )
    vmi_uid, launcher_uid = str(uuid4()), str(uuid4())
    fingerprint = "SHA256:" + "A" * 43
    observed = status(job, generation, boot=True, vmi_uid=vmi_uid)
    observed.update(
        vmi_uid=vmi_uid,
        ready=True,
        phase="Running",
        active_pod_uid=launcher_uid,
        pod_ip="10.42.0.90",
        interface_mac="02:00:00:00:00:41",
        ssh_host_key_fingerprint=fingerprint,
        credential_runtime_started=True,
    )

    async def reply(_request):
        return observed

    provisioner = await provisioner_with_response(db, monkeypatch, reply)
    monkeypatch.setattr(
        "orchestrator.services.vm_readiness.wait_for_agent_ssh",
        AsyncMock(return_value=(True, 1, None)),
    )
    qualifier = AsyncMock(
        return_value={
            "guest_boot_id": str(uuid4()),
            "guest_network": {
                "cloud_init_instance_id": "instance-one",
                "cloud_init_cached_instance_id": "instance-one",
                "network_profile_rule": {"network_file_sha256": "a" * 64},
            },
        }
    )
    monkeypatch.setattr(
        "orchestrator.services.vm_readiness.qualify_recovery_successor", qualifier
    )
    try:
        # The first signed status poll persists the admitted SSH pin. The next
        # readiness scan consumes that stored pin, as in the live boot loop.
        assert await provisioner.query_status(job) is not None
        await VMReadinessService(db, provisioner, trigger_dispatch=lambda: None)._probe(
            "job",
            job,
            generation,
            await vm_context(db, job),
            {"user_id": None},
            False,
        )
        stored = await vm_context(db, job)
        assert stored["status"] == "ready", stored.get("ssh_probe_error")
        assert stored["vmi_uid"] == vmi_uid
        assert stored["network_profile_evidence"]["vmi_uid"] == vmi_uid
        assert stored["network_profile_evidence"]["launcher_uid"] == launcher_uid
        assert qualifier.await_args.kwargs["network_profile"] == NETWORK_PROFILE
    finally:
        await provisioner._http_client.aclose()


@pytest.mark.asyncio
async def test_query_captures_revision_before_io_and_withholds_stale_reply(
    db, monkeypatch
):
    job, generation = await seed(db)

    async def reply(_request):
        newer = VMProvisioningPhaseStore(db)
        await newer.apply_status(
            await newer.capture(job, generation),
            status(job, generation, disk_progress=80),
        )
        return status(job, generation, disk_progress=10)

    provisioner = await provisioner_with_response(db, monkeypatch, reply)
    try:
        assert await provisioner.query_status(job) is None
    finally:
        await provisioner._http_client.aclose()
    assert (await vm_context(db, job))["provisioning"]["disk_progress_high_water"] == 80


@pytest.mark.asyncio
async def test_cancel_during_transport_prevents_phase_and_generic_identity_merge(
    db, monkeypatch
):
    job, generation = await seed(db)

    async def reply(_request):
        async with db.acquire() as conn:
            await conn.execute(
                "UPDATE jobs SET status='cancelled' WHERE id=$1", UUID(job)
            )
        return status(job, generation, boot=True)

    provisioner = await provisioner_with_response(db, monkeypatch, reply)
    try:
        assert await provisioner.query_status(job) is None
    finally:
        await provisioner._http_client.aclose()
    stored = await vm_context(db, job)
    assert "provisioning" not in stored
    assert "vm_uid" not in stored


@pytest.mark.asyncio
async def test_conflicting_signed_reply_is_not_returned_to_readiness(db, monkeypatch):
    job, generation = await seed(db, vm_fields={"vm_uid": evidence()["vm_uid"]})

    async def reply(_request):
        return status(job, generation, boot=True, vm_uid=str(uuid4()))

    provisioner = await provisioner_with_response(db, monkeypatch, reply)
    try:
        assert await provisioner.query_status(job) is None
    finally:
        await provisioner._http_client.aclose()
    assert (await vm_context(db, job))["vm_uid"] == evidence()["vm_uid"]


@pytest.mark.asyncio
async def test_deadline_expiring_during_job_lock_wait_withholds_phase_update(db):
    job, generation = await seed(db)
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO srw_execution_specs(id,work_kind,work_id,document,resolved,revision,harness_adapter) "
            "VALUES($1,'Job',$2,'{}','{\"spec\":{\"timeoutSeconds\":1}}','phase-test','srw/v1')",
            uuid4(),
            UUID(job),
        )
    store = VMProvisioningPhaseStore(db)
    token = await store.capture(job, generation)
    async with db.acquire() as blocking:
        async with blocking.transaction():
            await blocking.fetchval(
                "SELECT id FROM jobs WHERE id=$1 FOR UPDATE", UUID(job)
            )
            pending = asyncio.create_task(
                store.apply_status(token, status(job, generation, boot=True))
            )
            await asyncio.sleep(1.2)
    assert await pending == "held"
    assert "provisioning" not in await vm_context(db, job)


@pytest.mark.asyncio
@pytest.mark.parametrize("commit_during_lock_wait", [False, True])
async def test_real_recovery_admission_wins_against_inflight_phase(
    db, commit_during_lock_wait
):
    from orchestrator.services.vm_workspace_recovery_store import (
        VMWorkspaceRecoveryStore,
    )
    from tests.test_vm_workspace_recovery_real_postgres import (
        admission_kwargs,
        insert_leased_job,
    )

    job, lease = await insert_leased_job(db)
    kwargs = admission_kwargs(job, lease)
    generation = str(kwargs["provision_generation"])
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET context=jsonb_build_object('vm',jsonb_build_object("
            "'provision_generation',$2::text,'status','starting','vm_uid',$3::text,"
            "'rootdisk_pvc_uid',$4::text)) WHERE id=$1",
            job,
            generation,
            str(kwargs["vm_uid"]),
            str(kwargs["root_pvc_uid"]),
        )
    store = VMProvisioningPhaseStore(db)
    token = await store.capture(str(job), generation)
    reply = status(
        str(job),
        generation,
        vm_uid=str(kwargs["vm_uid"]),
        rootdisk_pvc_uid=str(kwargs["root_pvc_uid"]),
    )
    if commit_during_lock_wait:
        async with db.acquire() as blocking:
            async with blocking.transaction():
                await VMWorkspaceRecoveryStore(db).admit_hold(**kwargs, _conn=blocking)
                pending = asyncio.create_task(store.apply_status(token, reply))
                await asyncio.sleep(0.2)
                assert not pending.done()
        assert await pending == "held"
    else:
        await VMWorkspaceRecoveryStore(db).admit_hold(**kwargs)
        assert await store.apply_status(token, reply) == "held"
    assert "provisioning" not in await vm_context(db, str(job))


@pytest.mark.asyncio
@pytest.mark.parametrize("malformed", ["not-json", []])
@pytest.mark.parametrize(
    "path",
    [
        "workspace_storage",
        "creation_request",
        "creation_request.request",
        "creation_request.request.workspace_storage",
    ],
)
async def test_malformed_nested_authority_cannot_admit_first_identity(
    db, malformed, path
):
    fields = {}
    target = fields
    names = path.split(".")
    for key in names[:-1]:
        target[key] = {}
        target = target[key]
    target[names[-1]] = malformed
    job, generation = await seed(db, vm_fields=fields)
    store = VMProvisioningPhaseStore(db)
    result = await store.apply_status(
        await store.capture(job, generation), status(job, generation)
    )
    assert result in {"conflict", "held"}
    assert "vm_uid" not in await vm_context(db, job)


@pytest.mark.asyncio
async def test_new_generation_resets_phase_history_but_a_poll_does_not(db):
    from orchestrator.services.vm_provisioner import VMProvisioner
    from tests.test_non_pinned_workspace_lifecycle_real_postgres import _vm_process_zero

    job, generation = await seed(db)
    store = VMProvisioningPhaseStore(db)
    await store.apply_status(
        await store.capture(job, generation), status(job, generation, boot=True)
    )
    old = await vm_context(db, job)
    new = VMProvisioner._fresh_provision_ctx()
    async with db.acquire() as conn:
        await _vm_process_zero(conn, "job", UUID(job), generation)
    await db.merge_vm_context(job, new)
    next_generation = new["provision_generation"]
    token = await store.capture(job, next_generation)
    assert token.revision == 0
    assert await store.apply_status(token, status(job, next_generation)) == "observed"
    current = await vm_context(db, job)
    assert current["provisioning"]["first_guest_started_at"] is None
    assert old["provisioning"]["first_guest_started_at"] is not None
    assert await store.capture(job, generation) is None
