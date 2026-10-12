"""A terminal Job's browser IDE follows its exact completed restore receipt."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest

from orchestrator.services.ide_credentials import IDE_CREDENTIAL_ENV, ide_credential
from orchestrator.services.ide_proxy import IdeProxyService, contain_ide_status_for
from orchestrator.services.workspace_lifecycle import WorkspaceOwner
from tests import test_non_pinned_workspace_lifecycle_real_postgres as lifecycle

db = lifecycle.db
pg_dsn = lifecycle.pg_dsn
_schema_applied = lifecycle._schema_applied


@pytest.mark.asyncio
@pytest.mark.parametrize("owner_status", ["completed", "cancelled"])
async def test_current_settled_terminal_restore_advertises_exact_pod(
    db, monkeypatch, owner_status
):
    monkeypatch.setenv("IDE_CREDENTIAL_KEY", "disposable-test-key")
    job_id, runtime = uuid4(), uuid4()
    job_id_text, runtime_text = str(job_id), str(runtime)
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO jobs (id,description,status,repo_name,context) "
            "VALUES ($1,'terminal IDE proxy',$2,'owned-repo',$3::jsonb)",
            job_id,
            owner_status,
            json.dumps(
                {
                    "ide_session": {
                        "status": "restoring",
                        "source": "gitea",
                        "snapshot_type": "gitea",
                        "restore_type": "k8s_container",
                        "started_at": datetime.now(timezone.utc).isoformat(),
                    }
                }
            ),
        )
    reservation = await db.reserve_managed_repository_workspace_creation(
        job_id_text,
        owner_kind="job",
        scope="ide",
        operation_kind="restore",
        claimant="ide-proxy-issuer",
        desired_manifest_digest="a" * 64,
    )
    assert reservation is not None
    gate = dict(
        owner_kind="job",
        scope="ide",
        reservation_generation=reservation["reservation_generation"],
        claimant="ide-proxy-issuer",
        claim_token=reservation["claim_token"],
    )
    assert await db.mark_managed_repository_workspace_creation_started(
        job_id_text, **gate
    )
    assert await db.begin_managed_repository_workspace_creation_effect(
        job_id_text, **gate, resource_kind="pod"
    )
    assert await db.authorize_managed_repository_workspace_creation_runtime(
        job_id_text, **gate, runtime_incarnation=runtime_text
    )
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET context=jsonb_set(context,'{ide_session}',"
            "context->'ide_session' || $2::jsonb) WHERE id=$1",
            job_id,
            json.dumps(
                {
                    "_runtime_incarnation": runtime_text,
                    "_creation_reservation_id": str(reservation["id"]),
                    "_creation_claim_token": str(reservation["claim_token"]),
                    "container_name": f"ide-{job_id_text[:12]}",
                }
            ),
        )
    assert await db.merge_ide_session_context_if_runtime(
        job_id_text,
        {
            "status": "restoring",
            "pod_ip": "10.42.2.19",
            "pod_name": f"ide-{job_id_text[:12]}",
            "namespace": "agent-workspaces",
        },
        expected_runtime_incarnation=runtime_text,
    )
    assert await db.settle_managed_repository_workspace_creation_reservation(
        job_id_text, **gate, runtime_incarnation=runtime_text
    )
    claim = await db.claim_current_managed_repository_ide_restore_work(
        job_id_text, claimant="ide-proxy-test"
    )
    assert claim is not None
    assert await db.complete_managed_repository_ide_restore_work(
        job_id_text,
        reservation_id=str(reservation["id"]),
        runtime_incarnation=runtime_text,
        claimant="ide-proxy-test",
        work_claim_token=claim["restore_work_claim_token"],
        result_kind="active",
        code_server_url=f"/api/jobs/{job_id_text}/ide/proxy/",
        last_activity=datetime.now(timezone.utc).isoformat(),
    )
    receipt = await db.get_current_managed_repository_workspace_creation_result(
        job_id_text, owner_kind="job", scope="ide", operation_kind="restore"
    )
    assert receipt is not None and receipt["restore_work_result_kind"] == "active"
    # Foreground success clears the attempt marker; the owner-named settled
    # receipt remains the authority for this exact runtime.
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET context=context #- '{ide_session,_restore_attempt_id}' "
            "WHERE id=$1",
            job_id,
        )

    owner = WorkspaceOwner.job(job_id_text)
    credential = ide_credential(
        namespace="agent-workspaces",
        owner_kind=owner.kind,
        owner_id=owner.id,
        pod_name=f"ide-{job_id_text[:12]}",
    )
    pod = SimpleNamespace(
        metadata=SimpleNamespace(
            name=f"ide-{job_id_text[:12]}",
            namespace="agent-workspaces",
            uid=runtime_text,
            deletion_timestamp=None,
            labels={
                "srw/job-id": job_id_text,
                "app": "srw-workspace",
                "srw/component": "ide-session",
                "srw.io/component": "agent-workspace",
            },
        ),
        status=SimpleNamespace(
            phase="Running",
            pod_ip="10.42.2.19",
            container_statuses=[SimpleNamespace(ready=True)],
        ),
        spec=SimpleNamespace(
            containers=[
                SimpleNamespace(
                    env=[SimpleNamespace(name=IDE_CREDENTIAL_ENV, value=credential)]
                )
            ]
        ),
    )
    provisioner = SimpleNamespace(
        is_available=True,
        _namespace="agent-workspaces",
        _core_api=SimpleNamespace(read_namespaced_pod=MagicMock(return_value=pod)),
    )
    service = IdeProxyService()
    service.connect(db, provisioner)
    target = await service.resolve_target(job_id_text)
    assert target is not None and target.scope == "ide" and target.backend == "k8s"
    assert target.credential == credential
    assert await service.revalidate_target(target)
    status = {
        "status": "active",
        "code_server_url": f"/api/jobs/{job_id_text}/ide/proxy/",
    }
    with patch("orchestrator.services.ide_proxy.ide_proxy_service", service):
        assert await contain_ide_status_for(job_id_text, status) is status

    async with db.acquire() as conn:
        inserted = await conn.execute(
            "INSERT INTO managed_repository_workspace_cleanup_intents "
            "(owner_kind,owner_id,scope,runtime_incarnation,pod_uid,"
            "target_disposition) VALUES ('job',$1,'ide',$2,$2,'expired')",
            job_id,
            runtime,
        )
    assert inserted == "INSERT 0 1"
    assert await service.resolve_target(job_id_text) is None
    with patch("orchestrator.services.ide_proxy.ide_proxy_service", service):
        unavailable = await contain_ide_status_for(job_id_text, status)
    assert unavailable["status"] == "unavailable"
    assert unavailable["code_server_url"] is None
