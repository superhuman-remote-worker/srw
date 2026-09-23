"""Pinned job mutations are bound to one registered runtime process."""

from orchestrator.services.workspace_lifecycle import WorkspaceOwner
from tests import _b09_control_seams as control_seams

from unittest.mock import AsyncMock, patch

import pytest

from orchestrator import main
from agent.api.models import PinnedJobRecipient, pinned_job_recipient_matches


AGENT_ID = "11111111-1111-4111-8111-111111111111"
JOB_ID = "22222222-2222-4222-8222-222222222222"
PROCESS_GENERATION = "33333333-3333-4333-8333-333333333333"
POD_UID = "44444444-4444-4444-8444-444444444444"


class _Response:
    def __init__(self, payload: dict, status_code: int = 200):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        return self._payload


class _ReadyClient:
    payload = {
        "ready": True,
        "capabilities": {"pinned_recipient_binding": True},
    }
    urls: list[str] = []

    def __init__(self, *_args, **_kwargs):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False

    async def get(self, url):
        self.urls.append(url)
        return _Response(self.payload)


class _MutationClient:
    posts: list[tuple[str, dict]] = []

    def __init__(self, *_args, **_kwargs):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False

    async def post(self, url, json=None):
        self.posts.append((url, json))
        return _Response({}, status_code=202)


def _agent(*, pod_uid=None, status="ready", current_job_id=None) -> dict:
    return {
        "id": AGENT_ID,
        "hostname": "pod-a" if pod_uid else "local-agent",
        "pod_ip": "10.0.0.9",
        "pod_port": 8001,
        "pod_uid": pod_uid,
        "status": status,
        "current_job_id": current_job_id,
        "metadata": {"dispatch_process_generation": PROCESS_GENERATION},
    }


def test_runtime_identity_requires_all_four_exact_fields():
    recipient = PinnedJobRecipient(
        expected_agent_id=AGENT_ID,
        expected_pod_uid=POD_UID,
        expected_process_generation=PROCESS_GENERATION,
        expected_job_id=JOB_ID,
    )
    assert pinned_job_recipient_matches(
        recipient,
        agent_id=AGENT_ID,
        pod_uid=POD_UID,
        process_generation=PROCESS_GENERATION,
        job_id=JOB_ID,
    )
    assert not pinned_job_recipient_matches(
        recipient,
        agent_id=AGENT_ID,
        pod_uid="replacement-uid",
        process_generation=PROCESS_GENERATION,
        job_id=JOB_ID,
    )
    assert not pinned_job_recipient_matches(
        recipient,
        agent_id=AGENT_ID,
        pod_uid=POD_UID,
        process_generation="replacement-process",
        job_id=JOB_ID,
    )
    assert not pinned_job_recipient_matches(
        recipient,
        agent_id=AGENT_ID,
        pod_uid=POD_UID,
        process_generation=PROCESS_GENERATION,
        job_id="replacement-job",
    )
    assert not pinned_job_recipient_matches(
        None,
        agent_id=AGENT_ID,
        pod_uid=POD_UID,
        process_generation=PROCESS_GENERATION,
        job_id=JOB_ID,
    )


@pytest.mark.asyncio
async def test_local_recipient_requires_new_runtime_capability():
    _ReadyClient.urls = []
    with (
        patch.object(main.postgres_db, "get_agent", AsyncMock(return_value=_agent())),
        patch.object(main.httpx, "AsyncClient", _ReadyClient),
        patch.object(
            main.agent_provisioner,
            "attest_pinned_job_recipient",
            AsyncMock(),
        ) as attest,
    ):
        target = await main._prepare_pinned_job_mutation_target(
            agent_id=AGENT_ID,
            job_id=JOB_ID,
            require_idle=True,
        )

    assert target is not None
    assert target.recipient.expected_agent_id == AGENT_ID
    assert target.recipient.expected_pod_uid is None
    assert target.recipient.expected_process_generation == PROCESS_GENERATION
    assert target.recipient.expected_job_id == JOB_ID
    assert _ReadyClient.urls == ["http://10.0.0.9:8001/ready"]
    attest.assert_not_awaited()


@pytest.mark.asyncio
async def test_k8s_replacement_is_refused_after_capability_probe():
    _ReadyClient.urls = []
    attest = AsyncMock(return_value=False)
    with (
        patch.object(
            main.postgres_db,
            "get_agent",
            AsyncMock(return_value=_agent(pod_uid=POD_UID)),
        ),
        patch.object(main.httpx, "AsyncClient", _ReadyClient),
        patch.object(
            main.agent_provisioner,
            "attest_pinned_job_recipient",
            attest,
        ),
        patch.object(main.asyncio, "sleep", AsyncMock()) as sleep,
    ):
        target = await main._prepare_pinned_job_mutation_target(
            agent_id=AGENT_ID,
            job_id=JOB_ID,
            require_idle=True,
        )

    assert target is None
    assert attest.await_count == main._FRESH_PINNED_RECIPIENT_ATTESTATION_ATTEMPTS
    attest.assert_awaited_with(
        "pod-a",
        expected_pod_uid=POD_UID,
        expected_pod_ip="10.0.0.9",
    )
    assert sleep.await_count == main._FRESH_PINNED_RECIPIENT_ATTESTATION_ATTEMPTS - 1


@pytest.mark.asyncio
async def test_fresh_k8s_recipient_tolerates_readiness_publication_race():
    _ReadyClient.urls = []
    attest = AsyncMock(side_effect=[False, False, True])
    with (
        patch.object(
            main.postgres_db,
            "get_agent",
            AsyncMock(return_value=_agent(pod_uid=POD_UID)),
        ),
        patch.object(main.httpx, "AsyncClient", _ReadyClient),
        patch.object(
            main.agent_provisioner,
            "attest_pinned_job_recipient",
            attest,
        ),
        patch.object(main.asyncio, "sleep", AsyncMock()) as sleep,
    ):
        target = await main._prepare_pinned_job_mutation_target(
            agent_id=AGENT_ID,
            job_id=JOB_ID,
            require_idle=True,
        )

    assert target is not None
    assert attest.await_count == 3
    assert sleep.await_count == 2


@pytest.mark.asyncio
async def test_working_k8s_recipient_attestation_remains_fail_fast():
    attest = AsyncMock(return_value=False)
    with (
        patch.object(
            main.postgres_db,
            "get_agent",
            AsyncMock(
                return_value=_agent(
                    pod_uid=POD_UID,
                    status="working",
                    current_job_id=JOB_ID,
                )
            ),
        ),
        patch.object(main.httpx, "AsyncClient", _ReadyClient),
        patch.object(
            main.agent_provisioner,
            "attest_pinned_job_recipient",
            attest,
        ),
        patch.object(main.asyncio, "sleep", AsyncMock()) as sleep,
    ):
        target = await main._prepare_pinned_job_mutation_target(
            agent_id=AGENT_ID,
            job_id=JOB_ID,
            require_idle=False,
        )

    assert target is None
    attest.assert_awaited_once()
    sleep.assert_not_awaited()


@pytest.mark.asyncio
async def test_old_runtime_and_wrong_current_job_fail_before_mutation():
    old_payload = _ReadyClient.payload
    try:
        _ReadyClient.payload = {"ready": True, "capabilities": {}}
        with (
            patch.object(
                main.postgres_db,
                "get_agent",
                AsyncMock(return_value=_agent()),
            ),
            patch.object(main.httpx, "AsyncClient", _ReadyClient),
        ):
            assert (
                await main._prepare_pinned_job_mutation_target(
                    agent_id=AGENT_ID,
                    job_id=JOB_ID,
                    require_idle=True,
                )
                is None
            )

        network = AsyncMock()
        with (
            patch.object(
                main.postgres_db,
                "get_agent",
                AsyncMock(
                    return_value=_agent(
                        status="working", current_job_id="replacement-job"
                    )
                ),
            ),
            patch.object(main.httpx, "AsyncClient", network),
        ):
            assert (
                await main._prepare_pinned_job_mutation_target(
                    agent_id=AGENT_ID,
                    job_id=JOB_ID,
                    require_idle=False,
                )
                is None
            )
        network.assert_not_called()
    finally:
        _ReadyClient.payload = old_payload


@pytest.mark.asyncio
async def test_exact_working_job_is_a_safe_lost_response_retry():
    with (
        patch.object(
            main.postgres_db,
            "get_agent",
            AsyncMock(return_value=_agent(status="working", current_job_id=JOB_ID)),
        ),
        patch.object(main.httpx, "AsyncClient", _ReadyClient),
    ):
        target = await main._prepare_pinned_job_mutation_target(
            agent_id=AGENT_ID,
            job_id=JOB_ID,
            require_idle=True,
        )

    assert target is not None
    assert target.recipient.expected_job_id == JOB_ID


@pytest.mark.asyncio
async def test_fresh_start_delivers_hidden_recipient_before_existing_db_cas():
    _MutationClient.posts = []
    job = {
        "id": JOB_ID,
        "description": "bound start",
        "execution_lane": "pinned",
        "priority": 1,
    }
    selected = _agent()
    recipient = PinnedJobRecipient(
        expected_agent_id=AGENT_ID,
        expected_pod_uid=None,
        expected_process_generation=PROCESS_GENERATION,
        expected_job_id=JOB_ID,
    )
    start_request = main.JobStartRequest(job_id=JOB_ID, description="bound start")
    update_status = AsyncMock()
    heartbeat = AsyncMock()
    with (
        patch.object(
            main,
            "_prepare_job_workspace_runtime",
            AsyncMock(return_value=("proceed", job, None)),
        ),
        patch.object(
            main.job_workspace_authority,
            "attest_pinned_k8s_job_workspace",
            AsyncMock(return_value=(job, None)),
        ),
        patch.object(
            main.job_start_bundle,
            "build_job_start_request",
            AsyncMock(return_value=start_request),
        ),
        patch.object(
            main,
            "_workspace_runtime_unchanged_before_delivery",
            AsyncMock(return_value=True),
        ),
        patch.object(
            main.postgres_db,
            "managed_repository_authorities_are_current",
            AsyncMock(return_value=True),
        ),
        patch.object(
            main,
            "_prepare_pinned_job_mutation_target",
            AsyncMock(return_value=main._PinnedJobMutationTarget(selected, recipient)),
        ),
        patch.object(main.httpx, "AsyncClient", _MutationClient),
        patch.object(main.postgres_db, "update_job_status", update_status),
        patch.object(main.postgres_db, "heartbeat", heartbeat),
        patch.object(main, "COMPLETION_COMMANDS_ENABLED", False),
    ):
        assert await control_seams.dispatch_job_to_agent(job, selected)

    assert _MutationClient.posts == [
        (
            "http://10.0.0.9:8001/job/start",
            {
                "job_id": JOB_ID,
                "description": "bound start",
                "config_name": "worker_base",
                "recipient": recipient.model_dump(mode="json", exclude_none=True),
            },
        )
    ]
    update_status.assert_awaited_once_with(
        job_id=JOB_ID,
        status="processing",
        assigned_agent_id=AGENT_ID,
        expected_status="processing",
    )
    heartbeat.assert_awaited_once_with(
        agent_id=AGENT_ID,
        status="working",
        current_job_id=JOB_ID,
    )


@pytest.mark.asyncio
async def test_fresh_start_delivers_exact_k8s_authority_and_refuses_final_drift():
    _MutationClient.posts = []
    job = {
        "id": JOB_ID,
        "description": "attested start",
        "execution_lane": "pinned",
        "priority": 1,
    }
    selected = _agent()
    recipient = PinnedJobRecipient(
        expected_agent_id=AGENT_ID,
        expected_pod_uid=None,
        expected_process_generation=PROCESS_GENERATION,
        expected_job_id=JOB_ID,
    )
    attestation = main.WorkspaceRuntimeAttestation(
        backing_id="k8s-pvc:default:55555555-5555-4555-8555-555555555555",
        workspace_generation="55555555-5555-4555-8555-555555555555",
        runtime_incarnation=POD_UID,
        ssh_host_key_fingerprint="SHA256:" + "A" * 43,
        host="workspace-job.svc.cluster.local",
        pod_ip="10.42.0.9",
        port=30022,
    )
    authority = main._PinnedK8sJobWorkspaceAuthority(
        WorkspaceOwner.job(JOB_ID),
        attestation,
    )
    start_request = main.JobStartRequest(
        job_id=JOB_ID,
        description="attested start",
    )
    current = AsyncMock(side_effect=[True, False])
    target = AsyncMock(return_value=main._PinnedJobMutationTarget(selected, recipient))
    with (
        patch.object(
            main,
            "_prepare_job_workspace_runtime",
            AsyncMock(return_value=("proceed", job, None)),
        ),
        patch.object(
            main.job_workspace_authority,
            "attest_pinned_k8s_job_workspace",
            AsyncMock(return_value=(job, authority)),
        ),
        patch.object(
            main.job_start_bundle,
            "build_job_start_request",
            AsyncMock(return_value=start_request),
        ),
        patch.object(
            main.job_workspace_authority,
            "pinned_k8s_job_workspace_authority_is_current",
            current,
        ),
        patch.object(
            main.postgres_db,
            "managed_repository_authorities_are_current",
            AsyncMock(return_value=True),
        ),
        patch.object(
            main,
            "_prepare_pinned_job_mutation_target",
            target,
        ),
        patch.object(main.httpx, "AsyncClient", _MutationClient),
    ):
        assert await control_seams.dispatch_job_to_agent(job, selected) is False

    # Workspace authority is rechecked before recipient attestation, and the
    # recipient attestation is the final awaited authority check before HTTP.
    assert current.await_count == 2
    target.assert_not_awaited()
    assert _MutationClient.posts == []


@pytest.mark.asyncio
async def test_final_recheck_attests_inherited_parent_without_child_runtime_snapshot():
    parent_id = "55555555-5555-4555-8555-555555555555"
    child = {
        "id": JOB_ID,
        "parent_job_id": parent_id,
        "context": {"inherits_parent_workspace": True},
    }
    attestation = main.WorkspaceRuntimeAttestation(
        backing_id="k8s-pvc:default:66666666-6666-4666-8666-666666666666",
        workspace_generation="66666666-6666-4666-8666-666666666666",
        runtime_incarnation=POD_UID,
        ssh_host_key_fingerprint="SHA256:" + "A" * 43,
        host="workspace-parent.svc.cluster.local",
        pod_ip="10.42.0.9",
        port=30022,
    )
    authority = main._PinnedK8sJobWorkspaceAuthority(
        WorkspaceOwner.job(parent_id),
        attestation,
    )

    with (
        patch.object(
            main,
            "_workspace_runtime_unchanged_before_delivery",
            AsyncMock(return_value=True),
        ),
        patch.object(main.postgres_db, "get_job", AsyncMock(return_value=child)),
        patch.object(
            main.container_provisioner,
            "attest_workspace_runtime",
            AsyncMock(return_value=attestation),
        ) as attest,
    ):
        assert await control_seams.pinned_k8s_job_workspace_authority_is_current(
            child, authority
        )

    attest.assert_awaited_once_with(WorkspaceOwner.job(parent_id))
