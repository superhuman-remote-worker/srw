"""Idle consumers distinguish frozen caller intent from authenticated resolution."""

from copy import deepcopy
import asyncio
from datetime import datetime, timezone
import json
from uuid import uuid4

import pytest

from orchestrator.services.vm_idle_lifecycle import VMIdleLifecycleStore
from shared.vm_creation_retry import canonical_request_digest
from tests.test_vm_idle_admission_handoff_real_postgres import (
    db as _runtime_db,
    runtime_schema,  # noqa: F401
    _db_fixture,  # noqa: F401
    whole_schema,  # noqa: F401
    _base_db,  # noqa: F401
    _schema_applied,  # noqa: F401
    postgres_db_fixture,  # noqa: F401
    pg_dsn,  # noqa: F401
)

db = _runtime_db


async def resolved_idle_wait(db, monkeypatch, *, raw_disk=None, floor="30Gi"):
    from tests import test_vm_resource_job_runtime_real_postgres as runtime
    from tests import test_vm_resource_whole_store_real_postgres as sources

    original_configuration = sources.whole_launcher_configuration
    original_waiter = runtime.waiter

    def configuration():
        value = original_configuration()
        if floor is not None:
            value["disk_size_floor"] = floor
        return value

    async def waiter(*args, request_options=None, **kwargs):
        return await original_waiter(
            *args, request_options={**request_options, "disk_size": "30Gi"}, **kwargs
        )

    monkeypatch.setattr(sources, "whole_launcher_configuration", configuration)
    monkeypatch.setattr(runtime, "waiter", waiter)
    values = await runtime.charged_idle_wait(db, monkeypatch)
    _, retry, _, _, _ = values
    context = json.loads(await db.fetchval(
        "SELECT context FROM jobs WHERE id=$1", retry["job_id"]
    ))
    # Model complete_resolution's actual persisted shape: source/capture are
    # resolved, while the original preflight retains caller intent. No immutable
    # source or physical evidence is changed by this projection fixture.
    prior = context["vm"]["creation_preflight"]
    prior["admission_deadline"] = retry["admission_deadline"].isoformat()
    if raw_disk is None:
        prior["request"].pop("disk_size")
    else:
        prior["request"]["disk_size"] = raw_disk
    prior["request_digest"] = canonical_request_digest(prior["request"])
    await db.execute(
        "UPDATE jobs SET context=$2::jsonb WHERE id=$1",
        retry["job_id"], json.dumps(context),
    )
    return (*values, deepcopy(prior))


@pytest.mark.asyncio
@pytest.mark.parametrize("raw_disk", [None, "10Gi", "30Gi"])
async def test_actual_resolved_default_allows_native_idle_nomination(db, monkeypatch, raw_disk):
    _, retry, _, episode, identity, raw = await resolved_idle_wait(
        db, monkeypatch, raw_disk=raw_disk
    )
    source_before = dict(await db.fetchrow(
        "SELECT * FROM vm_creation_retries WHERE request_id=$1", retry["request_id"]
    ))
    operation = await VMIdleLifecycleStore(db).admit_release(
        str(retry["job_id"]), episode_id=episode.episode_id,
        revision=episode.revision, identity=identity,
    )
    assert operation is not None
    assert json.loads(await db.fetchval(
        "SELECT context->'vm'->'creation_preflight' FROM jobs WHERE id=$1",
        retry["job_id"],
    )) == raw
    assert dict(await db.fetchrow(
        "SELECT * FROM vm_creation_retries WHERE request_id=$1", retry["request_id"]
    )) == source_before


@pytest.mark.asyncio
@pytest.mark.parametrize("change", [
    "missing_floor", "explicit_disk_changed", "cpu_changed", "profile_changed",
    "request_id_changed", "deadline_changed", "preflight_digest_changed",
])
async def test_unproven_resolution_holds_before_idle_stop(db, monkeypatch, change):
    _, retry, _, episode, identity, _ = await resolved_idle_wait(
        db, monkeypatch, floor=None if change == "missing_floor" else "30Gi",
        raw_disk="40Gi" if change == "explicit_disk_changed" else None,
    )
    context = json.loads(await db.fetchval(
        "SELECT context FROM jobs WHERE id=$1", retry["job_id"]
    ))
    prior = context["vm"]["creation_preflight"]
    if change == "cpu_changed":
        prior["request"]["cpu_cores"] = 2
    elif change == "profile_changed":
        prior["request"].pop("network_profile")
    elif change == "request_id_changed":
        prior["request_id"] = str(uuid4())
        context["vm"]["creation_request_id"] = prior["request_id"]
    elif change == "deadline_changed":
        prior["admission_deadline"] = "2099-01-01T00:00:00+00:00"
    prior["request_digest"] = canonical_request_digest(prior["request"])
    if change == "preflight_digest_changed":
        prior["request_digest"] = "sha256:" + "0" * 64
    await db.execute(
        "UPDATE jobs SET context=$2::jsonb WHERE id=$1",
        retry["job_id"], json.dumps(context),
    )
    assert await VMIdleLifecycleStore(db).admit_release(
        str(retry["job_id"]), episode_id=episode.episode_id,
        revision=episode.revision, identity=identity,
    ) is None
    assert await db.fetchval("SELECT count(*) FROM vm_idle_operations") == 0
    assert await db.fetchval(
        "SELECT state FROM vm_resource_reservations WHERE request_id=$1", retry["request_id"]
    ) == "active"
    assert json.loads(await db.fetchval(
        "SELECT context FROM jobs WHERE id=$1", retry["job_id"]
    )) == context


@pytest.mark.asyncio
@pytest.mark.parametrize("change", [
    "request_id", "job_id", "provision_generation", "observed_vm_uid",
    "observed_pvc_uid", "request_digest", "controller_configuration_digest",
    "controller_configuration", "state",
])
async def test_resolved_source_proof_rejects_changed_authority(db, monkeypatch, change):
    from orchestrator.services.vm_creation_preflight import idle_wake_source_request
    from orchestrator.services.vm_creation_retry_store import VMCreationRetryConflict

    _, retry, _, _, _, _ = await resolved_idle_wait(db, monkeypatch)
    vm = json.loads(await db.fetchval(
        "SELECT context->'vm' FROM jobs WHERE id=$1", retry["job_id"]
    ))
    source = dict(await db.fetchrow(
        "SELECT * FROM vm_creation_retries WHERE request_id=$1", retry["request_id"]
    ))
    if change == "controller_configuration":
        source[change] = None
    elif change == "state":
        source[change] = "reconciling"
    elif change.endswith("digest"):
        source[change] = "sha256:" + "0" * 64
    else:
        source[change] = uuid4()
    # A contradictory current context capture cannot grant source authority.
    vm["creation_request"] = {"request": {"disk_size": "30Gi"}}
    with pytest.raises(VMCreationRetryConflict):
        idle_wake_source_request(vm, source)


@pytest.mark.asyncio
@pytest.mark.parametrize("lost_reply", [False, True])
async def test_native_wake_carries_resolved_options_and_original_provenance(
    db, monkeypatch, lost_reply,
):
    from tests import test_vm_idle_admission_handoff_real_postgres as handoff
    from orchestrator.services.vm_creation_preflight import VMCreationPreflightStore

    original = {}

    async def realistic_wait(db, monkeypatch):
        values = await resolved_idle_wait(db, monkeypatch)
        original.update(values[-1])
        return values[:-1]

    monkeypatch.setattr(handoff, "charged_idle_wait", realistic_wait)
    _, retry, operation, wake, _ = await handoff.suspended_charged_job(db, monkeypatch)
    source_before = dict(await db.fetchrow(
        "SELECT * FROM vm_creation_retries WHERE request_id=$1", retry["request_id"]
    ))
    # None of today's sizing defaults explains the predecessor's disk.
    monkeypatch.setenv("VM_DISK_SIZE", "200Gi")
    service = handoff.application_service(db, monkeypatch)
    begin = VMCreationPreflightStore.begin
    if lost_reply:
        async def lose_reply(self, **kwargs):
            await begin(self, **kwargs)
            raise RuntimeError("reply lost after committed native preflight")

        monkeypatch.setattr(VMCreationPreflightStore, "begin", lose_reply)
        assert await service.reconcile_once() == 0
        monkeypatch.setattr(VMCreationPreflightStore, "begin", begin)
        service = handoff.application_service(db, monkeypatch)
    else:
        assert await service.reconcile_once() == 1
    responses = await asyncio.gather(*[
        service.provisioner.create_vm(str(retry["job_id"]), idle_wake_id=str(operation["id"]))
        for _ in range(2)
    ])
    assert responses[0] == responses[1]
    context = json.loads(await db.fetchval(
        "SELECT context FROM jobs WHERE id=$1", retry["job_id"]
    ))
    successor = context["vm"]["creation_preflight"]
    assert context["last_vm"]["creation_preflight"] == original
    assert successor["request"]["disk_size"] == "30Gi"
    assert successor["request_id"] == str(wake["wake_request_id"])
    assert successor["request"]["provision_generation"] == str(wake["wake_generation"])
    for field in ("execution_id", "execution_revision", "execution_generation", "admission_deadline"):
        assert successor[field] == original[field]
    assert successor["predecessor_evidence"] == {
        "provision_generation": str(retry["provision_generation"]),
        "vm_uid": context["last_vm"]["vm_uid"],
    }
    assert await db.fetchval("SELECT count(*) FROM vm_creation_retries") == 1
    assert await db.fetchval("SELECT count(*) FROM vm_creation_effects") == 0
    assert dict(await db.fetchrow(
        "SELECT * FROM vm_creation_retries WHERE request_id=$1", retry["request_id"]
    )) == source_before


@pytest.mark.asyncio
async def test_native_resolution_persists_original_and_resolved_requests_separately(db):
    from tests.test_vm_creation_preflight_real_postgres import resolving

    job, store, claim, resolved = await resolving(db)
    assert "disk_size" not in claim["request"]
    assert resolved["request"]["disk_size"] == resolved["controller_configuration"]["disk_size_floor"]
    source = await store.complete_resolution(claim, resolved)
    vm = json.loads(await db.fetchval("SELECT context->'vm' FROM jobs WHERE id=$1", job))
    assert vm["creation_preflight"]["request"] == claim["request"]
    assert vm["creation_preflight"]["request_digest"] == claim["request_digest"]
    assert vm["creation_request"]["request"] == source["canonical_request"] == resolved["request"]
    assert source["controller_configuration"] == resolved["controller_configuration"]


@pytest.mark.asyncio
async def test_locked_wake_handoff_revalidates_after_advisory_read(db, monkeypatch):
    from tests import test_vm_idle_admission_handoff_real_postgres as handoff
    from orchestrator.services.vm_creation_preflight import VMCreationPreflightStore
    from orchestrator.services.vm_creation_retry_store import VMCreationRetryConflict

    async def realistic_wait(db, monkeypatch):
        return (await resolved_idle_wait(db, monkeypatch))[:-1]

    monkeypatch.setattr(handoff, "charged_idle_wait", realistic_wait)
    _, retry, operation, _, _ = await handoff.suspended_charged_job(db, monkeypatch)
    service = handoff.application_service(db, monkeypatch)
    begin = VMCreationPreflightStore.begin

    async def change_before_locked_begin(self, **kwargs):
        context = json.loads(await db.fetchval(
            "SELECT context FROM jobs WHERE id=$1", retry["job_id"]
        ))
        raw = context["vm"]["creation_preflight"]
        raw["request"]["disk_size"] = "40Gi"
        raw["request_digest"] = canonical_request_digest(raw["request"])
        await db.execute(
            "UPDATE jobs SET context=$2::jsonb WHERE id=$1", retry["job_id"], json.dumps(context)
        )
        return await begin(self, **kwargs)

    monkeypatch.setattr(VMCreationPreflightStore, "begin", change_before_locked_begin)
    with pytest.raises(VMCreationRetryConflict, match="idle_wake_predecessor_unproven"):
        await service.provisioner.create_vm(
            str(retry["job_id"]), idle_wake_id=str(operation["id"])
        )
    assert await db.fetchval(
        "SELECT context->'vm'->>'status' FROM jobs WHERE id=$1", retry["job_id"]
    ) == "suspended"
    assert await db.fetchval("SELECT count(*) FROM vm_creation_retries") == 1
    assert await db.fetchval("SELECT count(*) FROM vm_creation_effects") == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("current_floor", ["20Gi", "200Gi"])
async def test_wake_resolution_cannot_resize_retained_disk_with_current_defaults(
    db, monkeypatch, current_floor,
):
    from tests import test_vm_idle_admission_handoff_real_postgres as handoff
    from orchestrator.services.vm_creation_transport import CreationConfigurationUnavailable
    from vm_controller import controller as settings

    original = {}

    async def realistic_wait(db, monkeypatch):
        values = await resolved_idle_wait(db, monkeypatch)
        original.update(values[-1])
        return values[:-1]

    monkeypatch.setattr(handoff, "charged_idle_wait", realistic_wait)
    policy, retry, _, wake, identity = await handoff.suspended_charged_job(db, monkeypatch)
    source_before = dict(await db.fetchrow(
        "SELECT * FROM vm_creation_retries WHERE request_id=$1", retry["request_id"]
    ))
    assert await handoff.application_service(db, monkeypatch).reconcile_once() == 1
    monkeypatch.setattr(settings, "VM_DISK_SIZE", current_floor)
    if current_floor == "200Gi":
        with pytest.raises(CreationConfigurationUnavailable, match="creation_configuration_unproven"):
            await handoff.resolve_wake(db, monkeypatch, policy, wake, identity)
        assert await db.fetchval("SELECT count(*) FROM vm_creation_retries") == 1
    else:
        successor = await handoff.resolve_wake(db, monkeypatch, policy, wake, identity)
        assert successor["canonical_request"]["disk_size"] == "30Gi"
        assert successor["expected_pvc_uid"] == source_before["observed_pvc_uid"]
        assert successor["admission_deadline"] == source_before["admission_deadline"]
    context = json.loads(await db.fetchval(
        "SELECT context FROM jobs WHERE id=$1", retry["job_id"]
    ))
    assert context["last_vm"]["creation_preflight"] == original
    assert context["vm"]["creation_preflight"]["request"]["disk_size"] == "30Gi"
    assert dict(await db.fetchrow(
        "SELECT * FROM vm_creation_retries WHERE request_id=$1", retry["request_id"]
    )) == source_before
    assert await db.fetchval("SELECT count(*) FROM vm_creation_effects") == 0


@pytest.mark.asyncio
async def test_signed_changed_floor_records_attention_with_exact_claim(db, monkeypatch):
    from types import SimpleNamespace
    import httpx
    from tests import test_vm_idle_admission_handoff_real_postgres as handoff
    from tests.test_vm_resource_template import shipped_template
    from orchestrator.services.vm_creation_retry import VMCreationRetryService
    from orchestrator.services.vm_creation_retry_store import VMCreationRetryConflict
    from shared.vm_lifecycle_auth import AUTH_FIELD, sign_payload, unsigned_payload, verify_payload
    from vm_controller import controller as settings
    from vm_controller.creation_configuration import resolve_creation_configuration

    async def realistic_wait(db, monkeypatch):
        return (await resolved_idle_wait(db, monkeypatch))[:-1]

    monkeypatch.setattr(handoff, "charged_idle_wait", realistic_wait)
    _, retry, _, _, _ = await handoff.suspended_charged_job(db, monkeypatch)
    service = handoff.application_service(db, monkeypatch)
    assert await service.reconcile_once() == 1
    for key, value in {
        "VM_NAMESPACE": "workers", "VM_STORAGE_CLASS": "local",
        "VM_NODE_SELECTOR": {}, "VM_TOLERATIONS": [],
        "VM_PERSISTENT_ROOTDISK": True, "VM_DISK_SIZE": "200Gi",
    }.items():
        monkeypatch.setattr(settings, key, value)
    controller = SimpleNamespace(
        template_text=shipped_template(), cloud_init_text="#cloud-config",
        headscale=SimpleNamespace(is_available=False),
    )
    secret = b"signed-idle-resolution-test-secret-32"
    replies = []

    def signed_controller(request):
        body = json.loads(request.content)
        assert verify_payload(body, direction="request", operation="creation_config_resolve", secret=secret)
        reply = resolve_creation_configuration(controller, unsigned_payload(body)["request"])
        replies.append(reply)
        assert reply["request"]["disk_size"] == "200Gi"
        return httpx.Response(200, json=sign_payload(
            reply, direction="response", operation="creation_config_resolve",
            secret=secret, correlation_id=body[AUTH_FIELD]["request_id"],
        ))

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(signed_controller), base_url="http://controller",
    ) as client:
        worker = VMCreationRetryService(db, SimpleNamespace(
            _http_client=client, _lifecycle_hmac_secret=secret,
        ))
        claim = (await worker.preflight.claim_due(limit=1))[0]
        await worker._resolve(claim)
        vm = json.loads(await db.fetchval(
            "SELECT context->'vm' FROM jobs WHERE id=$1", retry["job_id"]
        ))
        value = vm["creation_preflight"]
        assert value["state"] == "attention"
        assert value["reason"] == "creation_configuration_unproven"
        assert value["attempt"] == claim["attempt"] + 1
        assert value["claim_token"] is None
        assert value["next_probe_at"] > value["outage_started_at"]
        assert value["next_probe_at"] - value["outage_started_at"] <= 300
        assert value["request"] == claim["request"]
        assert value["admission_deadline"] == claim["admission_deadline"]
        assert not await worker.preflight.record_failure(claim, reason="creation_configuration_unproven")
        with pytest.raises(VMCreationRetryConflict, match="creation_claim_stale"):
            await worker.preflight.complete_resolution(claim, replies[0])
    assert await db.fetchval("SELECT count(*) FROM vm_creation_retries") == 1
    assert await db.fetchval("SELECT count(*) FROM vm_creation_effects") == 0


@pytest.mark.asyncio
async def test_unchanged_source_deadline_expiry_still_refuses_native_wake(db, monkeypatch):
    from tests import test_vm_idle_admission_handoff_real_postgres as handoff
    from tests import test_vm_resource_whole_store_real_postgres as sources
    from orchestrator.services.vm_creation_retry_store import VMCreationRetryConflict

    original_admitted_job = sources.admitted_job

    async def short_deadline(*args, **kwargs):
        return await original_admitted_job(*args, timeout=5, **kwargs)

    async def realistic_wait(db, monkeypatch):
        return (await resolved_idle_wait(db, monkeypatch))[:-1]

    monkeypatch.setattr(sources, "admitted_job", short_deadline)
    monkeypatch.setattr(handoff, "charged_idle_wait", realistic_wait)
    _, retry, operation, _, _ = await handoff.suspended_charged_job(db, monkeypatch)
    source_before = dict(await db.fetchrow(
        "SELECT * FROM vm_creation_retries WHERE request_id=$1", retry["request_id"]
    ))
    await asyncio.sleep(max(0, (retry["admission_deadline"] - datetime.now(timezone.utc)).total_seconds()) + .02)
    service = handoff.application_service(db, monkeypatch)
    with pytest.raises(VMCreationRetryConflict, match="job_admission_expired"):
        await service.provisioner.create_vm(str(retry["job_id"]), idle_wake_id=str(operation["id"]))
    assert dict(await db.fetchrow(
        "SELECT * FROM vm_creation_retries WHERE request_id=$1", retry["request_id"]
    )) == source_before
    assert await db.fetchval("SELECT count(*) FROM vm_creation_retries") == 1
    assert await db.fetchval("SELECT count(*) FROM vm_creation_effects") == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("disk", [None, "not-a-quantity", "0Gi"])
async def test_unsized_historical_source_holds_usable_vm_before_stop(db, monkeypatch, disk):
    from tests import test_vm_resource_job_runtime_real_postgres as runtime

    original_waiter = runtime.waiter

    async def unsized_waiter(*args, request_options=None, **kwargs):
        options = {**request_options}
        if disk is None:
            options.pop("disk_size")
        else:
            options["disk_size"] = disk
        return await original_waiter(*args, request_options=options, **kwargs)

    monkeypatch.setattr(runtime, "waiter", unsized_waiter)
    _, retry, _, episode, identity = await runtime.charged_idle_wait(db, monkeypatch)
    source_before = dict(await db.fetchrow(
        "SELECT * FROM vm_creation_retries WHERE request_id=$1", retry["request_id"]
    ))
    assert await VMIdleLifecycleStore(db).admit_release(
        str(retry["job_id"]), episode_id=episode.episode_id,
        revision=episode.revision, identity=identity,
    ) is None
    assert await db.fetchval("SELECT count(*) FROM vm_idle_operations") == 0
    assert await db.fetchval(
        "SELECT state FROM vm_resource_reservations WHERE request_id=$1", retry["request_id"]
    ) == "active"
    assert dict(await db.fetchrow(
        "SELECT * FROM vm_creation_retries WHERE request_id=$1", retry["request_id"]
    )) == source_before
