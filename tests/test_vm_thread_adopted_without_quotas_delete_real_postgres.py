"""Non-quota physical VM creation keeps audit evidence through permanent End."""

import hashlib
import json
import logging
from types import SimpleNamespace
from uuid import uuid4
from unittest.mock import AsyncMock, MagicMock

import asyncpg
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


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "defect",
    [
        "source_generation",
        "source_actor",
        "vm_uid",
        "pvc_uid",
        "vm_uid_with_matching_purge",
        "pvc_uid_with_matching_purge",
        "configuration",
        "purge_digest",
        "purge_incomplete",
        "purge_outcome",
        "effect_issued",
        "other_cleanup_pending",
    ],
)
async def test_nonquota_audit_delete_refuses_inexact_source_or_unsettled_debt(
    db, setup, monkeypatch, defect
):
    current, source = await adopted_source(db, setup, monkeypatch)
    retirement = await cleaned_retirement(db, current, permanent=True)
    # Fault injection is confined to this disposable database. Bypass mutation
    # guards to independently test the final deletion authority against damaged
    # evidence; normal live writers cannot make these changes.
    async with db.acquire() as conn, conn.transaction():
        await conn.execute("SET LOCAL session_replication_role='replica'")
        if defect in {
            "source_generation",
            "source_actor",
            "vm_uid",
            "pvc_uid",
            "vm_uid_with_matching_purge",
            "pvc_uid_with_matching_purge",
        }:
            column = {
                "source_generation": "thread_runtime_generation",
                "source_actor": "thread_agent_id",
                "vm_uid": "observed_vm_uid",
                "pvc_uid": "observed_pvc_uid",
                "vm_uid_with_matching_purge": "observed_vm_uid",
                "pvc_uid_with_matching_purge": "observed_pvc_uid",
            }[defect]
            changed = uuid4() if column.startswith("thread_") else str(uuid4())
            await conn.execute(
                f"UPDATE vm_creation_retries SET {column}=$2 WHERE request_id=$1",
                source["request_id"],
                changed,
            )
            if defect.endswith("_with_matching_purge"):
                intent = {
                    "owner_id": str(current["id"]),
                    "owner_kind": "thread",
                    "provision_generation": str(source["provision_generation"]),
                    "purge_disk": True,
                    "pvc_uid": changed
                    if column == "observed_pvc_uid"
                    else str(source["observed_pvc_uid"]),
                    "resource": "vm_workspace",
                    "source": "pinned_thread_retirement",
                    "vm_uid": changed
                    if column == "observed_vm_uid"
                    else str(source["observed_vm_uid"]),
                }
                digest = (
                    "sha256:"
                    + hashlib.sha256(
                        json.dumps(
                            intent, sort_keys=True, separators=(",", ":")
                        ).encode()
                    ).hexdigest()
                )
                await conn.execute(
                    "UPDATE vm_workspace_cleanup_admissions SET intent_digest=$2,pvc_uid=$3 "
                    "WHERE owner_kind='thread' AND owner_id=$1 AND source='pinned_thread_retirement'",
                    current["id"],
                    digest,
                    intent["pvc_uid"],
                )
        elif defect == "configuration":
            await conn.execute(
                "UPDATE vm_creation_retries SET controller_configuration=jsonb_set("
                "controller_configuration,'{version}','3') WHERE request_id=$1",
                source["request_id"],
            )
        elif defect in {"purge_digest", "purge_incomplete", "purge_outcome"}:
            change = {
                "purge_digest": "intent_digest='unproven'",
                "purge_incomplete": "completed_at=NULL,outcome=NULL",
                "purge_outcome": "outcome='adopted'",
            }[defect]
            await conn.execute(
                f"UPDATE vm_workspace_cleanup_admissions SET {change} "
                "WHERE owner_kind='thread' AND owner_id=$1 AND source='pinned_thread_retirement'",
                current["id"],
            )
        elif defect == "effect_issued":
            await conn.execute(
                "UPDATE vm_creation_effects SET state='issued',evidence='{}'::jsonb,resolved_at=NULL "
                "WHERE request_id=$1 AND effect_kind='vm'",
                source["request_id"],
            )
        else:
            await conn.execute(
                "INSERT INTO vm_workspace_cleanup_admissions "
                "(id,owner_kind,owner_id,pvc_uid,source,request_id,intent_digest) "
                "VALUES($1,'thread',$2,$3,'pinned_thread_retirement',$4,'unproven')",
                uuid4(),
                current["id"],
                source["observed_pvc_uid"],
                uuid4(),
            )
    with pytest.raises(asyncpg.CheckViolationError):
        await db.delete_thread(
            str(current["id"]),
            expected_runtime_generation=retirement["generation"],
            expected_runtime_retirement_token=retirement["token"],
        )
    assert await db.get_thread(str(current["id"])) is not None
    owner = await db.fetchrow(
        "SELECT * FROM vm_thread_creation_owners WHERE thread_id=$1", current["id"]
    )
    assert owner["live_thread_id"] == current["id"] and owner["deleted_at"] is None


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
    if vm is None:
        captured_retained = await db.fetchval(
            "SELECT op.retained_vm FROM vm_thread_retained_resumes op JOIN threads t ON t.id=op.thread_id "
            "WHERE op.thread_id=$1 AND public.valid_vm_thread_retained_runtime(op,t)",
            current["id"],
        )
        assert captured_retained is not None
        vm = json.loads(captured_retained)

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
        if (
            kwargs["parent_cleanup"]["intent"]["source"]
            != "pinned_thread_retained_disk_purge"
        ):
            assert await db.merge_thread_vm_context_if_provision_generation(
                thread_id,
                identity.provision_generation,
                {"status": "retiring_process_zero"},
            )
        return VMTeardownResult("completed", True)

    async def attest_vm_cleanup_stop(candidate):
        assert candidate["owner_id"] == thread_id
        assert candidate["vm_uid"] == vm["vm_uid"]
        assert candidate["pvc_uid"] == vm["rootdisk_pvc_uid"]
        assert candidate["provision_generation"] == vm["provision_generation"]
        assert candidate["purge_disk"] is permanent
        return {
            "version": 1,
            "kind": "vm_cleanup_physical_stop",
            **{key: value for key, value in candidate.items() if key != "purge_disk"},
            "vm_absent": True,
            "vmi_absent": True,
            "launcher_absent": True,
            "same_generation_replacement": False,
            "controller_authenticated": True,
            "pvc_disposition": "purged" if permanent else "retained",
        }

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
                attest_vm_cleanup_stop=attest_vm_cleanup_stop,
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
    if not permanent:
        assert await db.merge_thread_vm_context_if_provision_generation(
            thread_id, vm["provision_generation"], {"status": "deleted"}
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
