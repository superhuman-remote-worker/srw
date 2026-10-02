"""Non-quota physical VM creation keeps audit evidence through permanent End."""

import json
import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from orchestrator.services.pinned_retirement import PinnedRetirementOperations
from orchestrator.services.session_router import SessionRouterService
from orchestrator.services.vm_provisioner import VMTeardownResult
from orchestrator.services.vm_workspace_recovery_store import VMWorkspaceRecoveryStore
from tests.test_vm_thread_cancel_without_quotas_real_postgres import (
    _base_db,  # noqa: F401
    _schema_applied,  # noqa: F401
    cancelled_source,
    db as _db,
    pg_dsn,  # noqa: F401
    setup as _setup,
    thread_schema,  # noqa: F401
)
from vm_controller.creation_actuation import CreationActuator
from vm_controller.creation_configuration import resolve_creation_configuration
from shared.vm_creation_issuance import canonical_configuration_digest
from kubernetes.client.exceptions import ApiException

db = _db
setup = _setup


async def adopted_source(db, setup, monkeypatch):
    controller, api, _, old_payload = setup
    retry, row, current, _ = await cancelled_source(
        db,
        monkeypatch,
        retire=False,
        golden_enabled=False,
        creation_resolver=lambda request: resolve_creation_configuration(
            controller, request
        ),
    )
    render = controller.render_template

    def render_thread(*args):
        return json.loads(
            json.dumps(render(*args)).replace(old_payload["job_id"], str(current["id"]))
        )

    controller.render_template = render_thread

    async def authority(path, body, *, operation):
        method = path.rsplit("/", 1)[-1].replace("-", "_")
        assert operation == "creation_retry_" + method
        return await getattr(
            retry, "authorize_controller" if method == "authorize" else method
        )(**body)

    controller._workspace_cleanup_authority_request = authority
    resolved = resolve_creation_configuration(controller, row["request"])
    assert (
        canonical_configuration_digest(resolved["controller_configuration"])
        == row["controller_configuration_digest"]
    )
    assert resolved["controller_configuration"] == row["controller_configuration"], [
        key
        for key in set(resolved["controller_configuration"])
        | set(row["controller_configuration"])
        if resolved["controller_configuration"].get(key)
        != row["controller_configuration"].get(key)
    ]
    payload = {
        **row["request"],
        "creation_retry": {
            "version": 1,
            "claim_token": str(
                await db.fetchval(
                    "SELECT claim_token FROM vm_creation_retries WHERE request_id=$1::uuid",
                    row["request_id"],
                )
            ),
            **{
                key: row[key]
                for key in (
                    "request_id",
                    "request_digest",
                    "controller_configuration_digest",
                )
            },
        },
    }
    actuator = CreationActuator(controller)
    for _ in range(6):
        result = await actuator._run(payload)
        if result["status"] == "created":
            break
        for (kind, _name), obj in api.objects.items():
            if kind == "DataVolume":
                obj["status"] = {"phase": "Succeeded"}
            elif kind == "PersistentVolumeClaim":
                obj["status"] = {"phase": "Bound"}
    else:
        final = await retry.inspect(request_id=row["request_id"])
        pytest.fail(
            str(
                {
                    "result": result,
                    "effects": [
                        (e["carrier_intent"]["effect_kind"], e["state"])
                        for e in final["effects"]
                    ],
                }
            )
        )
    current = await db.get_thread(str(current["id"]))
    source = await db.fetchrow(
        "SELECT * FROM vm_creation_retries WHERE request_id=$1::uuid", row["request_id"]
    )
    assert source["state"] == "succeeded" and source["reason"] == "creation_adopted"
    assert source["observed_vm_uid"] is not None and source["boot_counted"]
    assert json.loads(source["controller_configuration"])["version"] == 1
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_resource_reservations WHERE request_id=$1",
            source["request_id"],
        )
        == 0
    )
    return current, dict(source)


async def cleaned_retirement(db, current, *, permanent):
    thread_id = str(current["id"])
    retirement = await db.begin_pinned_thread_retirement(thread_id, permanent=permanent)
    assert retirement["state"] == "pending", retirement
    assert await db.authorize_pinned_thread_retirement(
        thread_id,
        token=retirement["token"],
        generation=retirement["generation"],
        settle_status="ended",
    )
    vm = retirement["context"]["vm"]

    async def stop_vm(owner, identity, **kwargs):
        assert owner == thread_id
        assert identity.vm_uid == vm["vm_uid"]
        assert identity.provision_generation == vm["provision_generation"]
        assert identity.rootdisk_pvc_uid == vm["rootdisk_pvc_uid"]
        assert kwargs["purge_disk"] is permanent
        assert await db.record_managed_repository_workspace_process_zero(
            thread_id,
            owner_kind="thread",
            scope="vm",
            provisioner="vm",
            runtime_incarnation=identity.provision_generation,
        )
        assert await db.merge_thread_vm_context_if_provision_generation(
            thread_id,
            identity.provision_generation,
            {"status": "retiring_process_zero"},
        )
        return VMTeardownResult("completed", True)

    # Only external process absence is modeled. All cleanup admissions, exact
    # G/T receipts, endpoint CAS, settlement and audit triggers use PostgreSQL.
    agent = SimpleNamespace(
        is_available=True,
        delete_agent_pod_exact=AsyncMock(return_value=True),
        agent_pod_authority=AsyncMock(return_value="exact_absent"),
        retire_historical_claimant_pod_exact=AsyncMock(return_value=True),
    )
    core, networking = MagicMock(), MagicMock()
    core.read_namespaced_service.side_effect = ApiException(status=404)
    networking.read_namespaced_ingress.side_effect = ApiException(status=404)
    operations = PinnedRetirementOperations(
        SimpleNamespace(
            store=db,
            vm_provisioner=SimpleNamespace(
                lifecycle_available=True,
                release_vm_captured=stop_vm,
            ),
            agent_provisioner=agent,
            session_router=SessionRouterService(
                namespace="agents-a",
                ingress_host="unused.test",
                core_api=core,
                networking_api=networking,
            ),
            recovery_store=VMWorkspaceRecoveryStore(db),
            logger=logging.getLogger(__name__),
        )
    )
    if current["agent_id"] is not None:
        assert await operations.recover_captured_process_zero(retirement)
    await operations.cleanup_pinned_thread_retirement(
        retirement, cleanup_agent_pod=True
    )
    return retirement


@pytest.mark.asyncio
@pytest.mark.parametrize("soft_first", [False, True])
async def test_adopted_nonquota_vm_permanent_delete_preserves_exact_audit(
    db,
    setup,
    monkeypatch,
    soft_first,  # noqa: F811
):
    current, source = await adopted_source(db, setup, monkeypatch)
    retirement = await cleaned_retirement(db, current, permanent=not soft_first)
    if soft_first:
        assert await db.settle_pinned_thread_retirement(
            str(current["id"]),
            token=retirement["token"],
            generation=retirement["generation"],
            final_status="ended",
        )
        current = await db.get_thread(str(current["id"]))
        retirement = await cleaned_retirement(db, current, permanent=True)
    await db.delete_thread(
        str(current["id"]),
        expected_runtime_generation=retirement["generation"],
        expected_runtime_retirement_token=retirement["token"],
    )
    assert await db.get_thread(str(current["id"])) is None
    audit = await db.fetchrow(
        "SELECT * FROM vm_thread_creation_owners WHERE thread_id=$1", current["id"]
    )
    assert audit["live_thread_id"] is None and audit["deleted_at"] is not None
    assert str(audit["deleted_retirement_token"]) == retirement["token"]
    assert (
        dict(
            await db.fetchrow(
                "SELECT * FROM vm_creation_retries WHERE request_id=$1",
                source["request_id"],
            )
        )
        == source
    )
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_thread_creation_settlements WHERE thread_id=$1",
            current["id"],
        )
        == 0
    )
