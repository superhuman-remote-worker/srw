"""Regression tests for the resume endpoint's delegation to the dispatcher path.

Covers the 2026-07-17 incident where the cockpit Resume button's direct
fast-path POSTed the bare persisted ``jobs.config_override`` (creation-time,
no env_keys / LLM credentials / workspace config) straight to the agent's
``/job/resume``. Jobs landing on a fresh (clean-env) agent pod then failed —
reranker bind hard-fail or missing workspace SSH config (5 jobs killed).

The fix delegates the fast-path to ``_resume_job_on_agent`` — the dispatcher's
resume path, which performs the full dispatch-time injection — so these tests
pin (a) that the shared path injects before POSTing, and (b) that the endpoint
actually delegates to it and falls back to queue-for-dispatch on decline.

See knowledge-base/knowledge/issues/job_resume_direct_path_skips_credential_injection.md.
"""

from __future__ import annotations

from tests import _b09_control_seams as control_seams

from tests._expert_catalog import patch_service_method
from orchestrator.services import expert_catalog as expert_catalog_module


import json
import os
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import ANY, AsyncMock, MagicMock

import pytest
from fastapi import HTTPException

os.environ.setdefault("VECTOR_DB_URL", "postgresql://test@localhost/test")

import orchestrator.main  # noqa: E402
from shared.runtime_actor import RuntimeActorContext  # noqa: E402
from shared.runtime.core.tool_policy import ToolPolicyError  # noqa: E402


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

JOB_ID = "00000000-0000-0000-0000-000000000001"
AGENT_ID = "00000000-0000-0000-0000-0000000000a1"
PROJECT_ID = "00000000-0000-0000-0000-0000000000bb"
WORKSPACE_RUNTIME = "11111111-1111-4111-8111-111111111111"

INJECTED_ENV = {
    "EMBEDDING_MODEL": "qwen3-embedding-8b",
    "EMBEDDING_BASE_URL": "http://router/v1",
    "EMBEDDING_API_KEY": "sk-emb",
}


def _job(**overrides) -> dict:
    job = {
        "id": JOB_ID,
        "user_id": None,
        "project_id": None,
        "config_name": "default",
        "config_override": {
            "llm": {"model": "gpt-5.6-terra"},
            "workspace": {"backend": "sandbox"},
        },
        "context": {
            "_workspace_contract": {
                "version": 1,
                "requested_backend": "sandbox",
                "assigned_backend": "sandbox",
                "assignment_source": "test",
            },
            "workspace_container": {
                "status": "ready",
                "host": "workspace-job.svc.cluster.local",
                "port": 30022,
                "provisioner": "k8s",
                "_runtime_incarnation": WORKSPACE_RUNTIME,
            },
        },
        "status": "paused",
        "assigned_agent_id": None,
        "priority": 1,
    }
    context_override = overrides.pop("context", None)
    config_override = overrides.pop("config_override", None)
    job.update(overrides)
    if config_override is not None:
        job["config_override"] = {
            **job["config_override"],
            **config_override,
        }
    if context_override is not None:
        context = {**job["context"], **context_override}
        if isinstance(context_override.get("workspace_container"), dict):
            context["workspace_container"] = {
                **job["context"]["workspace_container"],
                **context_override["workspace_container"],
            }
        job["context"] = context
    return job


def _agent(**overrides) -> dict:
    agent = {
        "id": AGENT_ID,
        "status": "ready",
        "pod_ip": "10.0.0.9",
        "pod_port": 8080,
    }
    agent.update(overrides)
    return agent


class _FakeResponse:
    def __init__(self, status_code: int, text: str = "", payload: dict | None = None):
        self.status_code = status_code
        self.text = text
        self.payload = payload or {}

    def json(self):
        return self.payload


class _FakeAsyncClient:
    """Stand-in for httpx.AsyncClient capturing the resume POST."""

    posts: list[tuple[str, dict]] = []
    next_status: int = 202
    next_text: str = ""
    resolved_config_resume: bool = True

    def __init__(self, *args, **kwargs):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def post(self, url, json=None):
        _FakeAsyncClient.posts.append((url, json))
        return _FakeResponse(_FakeAsyncClient.next_status, _FakeAsyncClient.next_text)

    async def get(self, url):
        return _FakeResponse(
            200,
            payload={
                "capabilities": {
                    "resolved_config_resume": self.resolved_config_resume,
                }
            },
        )


@pytest.fixture
def fake_conn(monkeypatch):
    """postgres_db.acquire() -> conn with a recorded execute()."""
    conn = SimpleNamespace(execute=AsyncMock(), fetchrow=AsyncMock(return_value=None))

    @asynccontextmanager
    async def _acquire():
        yield conn

    monkeypatch.setattr(orchestrator.main.postgres_db, "acquire", _acquire)
    return conn


@pytest.fixture
def injector(monkeypatch):
    """Replace `_inject_dispatch_credentials` with a marker injector."""

    async def _fake(job, config_override, *, include_kb_profile=False):
        config_override = config_override or {}
        config_override.setdefault("env_keys", {}).update(INJECTED_ENV)
        config_override.setdefault("llm", {})["api_key"] = "sk-llm"
        return config_override

    mock = AsyncMock(side_effect=_fake)
    monkeypatch.setattr(orchestrator.main, "_inject_dispatch_credentials", mock)
    return mock


@pytest.fixture
def resume_collaborators(monkeypatch, fake_conn, injector):
    """Patch every DB/transport collaborator of `_resume_job_on_agent`."""
    _FakeAsyncClient.posts = []
    _FakeAsyncClient.next_status = 202
    _FakeAsyncClient.next_text = ""
    _FakeAsyncClient.resolved_config_resume = True
    monkeypatch.setattr(orchestrator.main.httpx, "AsyncClient", _FakeAsyncClient)

    # Most tests in this fixture exercise the legacy flat-override fallback.
    # Resolved-config delivery gets a focused regression below.
    monkeypatch.setattr(orchestrator.main, "_is_experts_db_enabled", lambda: False)
    monkeypatch.setattr(
        orchestrator.main, "_user_experts_enabled", AsyncMock(return_value=False)
    )
    monkeypatch.setattr(
        orchestrator.main,
        "_resolve_authorized_job_datasources",
        AsyncMock(return_value=[]),
    )
    monkeypatch.setattr(
        orchestrator.main, "_job_project_repositories", AsyncMock(return_value=None)
    )
    monkeypatch.setattr(orchestrator.main, "_apply_cloud_storage_override", MagicMock())
    monkeypatch.setattr(
        orchestrator.main, "_build_datasources_payload", MagicMock(return_value=[])
    )
    monkeypatch.setattr(
        orchestrator.main,
        "_build_datasource_tool_override",
        MagicMock(side_effect=lambda ds, co: co),
    )
    monkeypatch.setattr(
        orchestrator.main.postgres_db, "delete_job_context_keys", AsyncMock()
    )
    monkeypatch.setattr(orchestrator.main.postgres_db, "update_job_status", AsyncMock())
    monkeypatch.setattr(orchestrator.main.postgres_db, "heartbeat", AsyncMock())
    monkeypatch.setattr(
        orchestrator.main.container_provisioner,
        "attest_workspace_runtime",
        AsyncMock(
            return_value=orchestrator.main.WorkspaceRuntimeAttestation(
                backing_id="k8s-pvc:agent-workspaces:workspace-pvc",
                workspace_generation="22222222-2222-4222-8222-222222222222",
                runtime_incarnation=WORKSPACE_RUNTIME,
                ssh_host_key_fingerprint="SHA256:resume-workspace",
                host="workspace-job.svc.cluster.local",
                pod_ip="10.42.0.17",
                port=30022,
            )
        ),
    )
    monkeypatch.setattr(
        orchestrator.main.job_workspace_authority,
        "pinned_k8s_job_workspace_authority_is_current",
        AsyncMock(return_value=True),
    )
    recipient = orchestrator.main.PinnedJobRecipient(
        expected_agent_id=AGENT_ID,
        expected_pod_uid=None,
        expected_process_generation="55555555-5555-4555-8555-555555555555",
        expected_job_id=JOB_ID,
    )
    monkeypatch.setattr(
        orchestrator.main,
        "_prepare_pinned_job_mutation_target",
        AsyncMock(
            return_value=orchestrator.main._PinnedJobMutationTarget(
                _agent(), recipient
            ),
        ),
    )
    monkeypatch.setattr(
        orchestrator.main.postgres_db,
        "managed_repository_authorities_are_current",
        AsyncMock(return_value=True),
    )
    monkeypatch.setattr(
        orchestrator.main,
        "_workspace_runtime_unchanged_before_delivery",
        AsyncMock(return_value=True),
    )

    async def _mint_worker(_db, *, project_id, user_id):
        return RuntimeActorContext(
            caller_kind="worker",
            project_id=project_id,
            user_id=user_id,
            access_credential="sra_" + ("A" * 43),
            refresh_credential="srr_" + ("B" * 43),
        )

    monkeypatch.setattr(
        orchestrator.main,
        "mint_worker_runtime_actor",
        AsyncMock(side_effect=_mint_worker),
    )
    return SimpleNamespace(conn=fake_conn, injector=injector)


def _posted_payload() -> dict:
    assert len(_FakeAsyncClient.posts) == 1, "expected exactly one resume POST"
    return _FakeAsyncClient.posts[0][1]


# ---------------------------------------------------------------------------
# _resume_job_on_agent — the shared payload builder
# ---------------------------------------------------------------------------


class TestResumeJobOnAgentInjection:
    @pytest.mark.asyncio
    async def test_vm_resume_dispatch_binds_agent_receipt_before_confirmation(
        self, resume_collaborators, monkeypatch
    ):
        """A resumed VM claim must give its recipient a new report identity."""
        from uuid import uuid4

        from agent.api.models import JobResumeRequest
        from agent.api.pinned_delivery import accepted_pinned_job_delivery

        delivery_id = uuid4()
        monkeypatch.setenv("WORKSPACE_IDLE_RELEASE_ENABLED", "true")
        monkeypatch.setenv("VM_LIFECYCLE_HMAC_SECRET", "x" * 64)
        monkeypatch.setattr(orchestrator.main, "COMPLETION_COMMANDS_ENABLED", True)
        monkeypatch.setattr(
            orchestrator.main.postgres_db, "prepare_pinned_job_delivery",
            AsyncMock(return_value={"id": delivery_id}),
        )
        confirm = AsyncMock(return_value=True)
        monkeypatch.setattr(
            orchestrator.main.postgres_db, "confirm_pinned_job_dispatch", confirm,
        )
        recipient = orchestrator.main.PinnedJobRecipient(
            expected_agent_id=AGENT_ID,
            expected_pod_uid="44444444-4444-4444-8444-444444444444",
            expected_process_generation="55555555-5555-4555-8555-555555555555",
            expected_job_id=JOB_ID,
        )
        monkeypatch.setattr(
            orchestrator.main, "_prepare_pinned_job_mutation_target",
            AsyncMock(return_value=orchestrator.main._PinnedJobMutationTarget(_agent(), recipient)),
        )
        accepted_client = SimpleNamespace()

        async def accepted_post(_self, url, json=None):
            assert url.endswith("/job/resume")
            wire = JobResumeRequest.model_validate(json)
            echo = accepted_pinned_job_delivery(wire, accepted_client, retry=False)
            _FakeAsyncClient.posts.append((url, json))
            return _FakeResponse(202, payload=echo)

        monkeypatch.setattr(_FakeAsyncClient, "post", accepted_post)
        vm_job = _job(
            config_override={"workspace": {"backend": "vm"}},
            context={
                "_workspace_contract": {
                    "version": 1, "requested_backend": "vm",
                    "assigned_backend": "vm", "assignment_source": "request",
                },
                "vm": {
                    "status": "ready", "provisioner": "vm",
                    "ssh_host": "100.64.0.7", "ssh_port": 22,
                    "provision_generation": WORKSPACE_RUNTIME,
                },
            },
        )
        assert await control_seams.resume_job_on_agent(vm_job, _agent()) is True
        assert str(accepted_client.pinned_delivery_id) == str(delivery_id)
        assert confirm.await_args.kwargs["pinned_delivery_id"] == str(delivery_id)
        assert confirm.await_args.kwargs["pinned_projection_digest"] == (
            accepted_client.pinned_projection_digest
        )

    @pytest.mark.asyncio
    async def test_sandbox_resume_rebuilds_complete_ssh_config(
        self, resume_collaborators, monkeypatch
    ):
        monkeypatch.delenv("SSH_KEY_PATH", raising=False)
        job = _job(
            config_override={"workspace": {"backend": "sandbox"}},
            context={
                "workspace_container": {
                    "status": "ready",
                    "host": "workspace-job.svc.cluster.local",
                    "port": 30022,
                    "provisioner": "k8s",
                }
            },
            worktree_path="/home/agent-host/workspace/.worktrees/job-1",
        )

        assert await control_seams.resume_job_on_agent(job, _agent()) is True

        remote = _posted_payload()["config_override"]["workspace"]["remote"]
        assert remote == {
            "host": "workspace-job.svc.cluster.local",
            "port": 30022,
            "username": "agent-host",
            "key_path": "/run/secrets/vm-ssh-key",
            "workspace_path": "/home/agent-host/workspace/.worktrees/job-1",
        }
        assert _posted_payload()["config_override"]["shell"]["sudo_action"] == "freeze"

    @pytest.mark.asyncio
    async def test_docker_resume_uses_docker_key_mount(
        self, resume_collaborators, monkeypatch
    ):
        monkeypatch.delenv("SSH_KEY_PATH", raising=False)
        job = _job(
            config_override={"workspace": {"backend": "sandbox"}},
            context={
                "workspace_container": {
                    "status": "ready",
                    "pod_ip": "172.20.0.8",
                    "port": 22,
                    "provisioner": "docker",
                    "_docker_workspace_lease_id": (
                        "22222222-2222-4222-8222-222222222222"
                    ),
                    "_docker_workspace_attested": True,
                }
            },
        )

        assert await control_seams.resume_job_on_agent(job, _agent()) is True
        remote = _posted_payload()["config_override"]["workspace"]["remote"]
        assert remote["key_path"] == "/run/secrets/ssh/id_ed25519"

    @pytest.mark.asyncio
    async def test_injected_credentials_reach_the_posted_payload(
        self, resume_collaborators
    ):
        ok = await control_seams.resume_job_on_agent(
            _job(project_id=PROJECT_ID), _agent()
        )

        assert ok is True
        payload = _posted_payload()
        assert "resolved_config" not in payload
        assert payload["config_override"]["env_keys"] == INJECTED_ENV
        assert payload["config_override"]["llm"]["api_key"] == "sk-llm"
        assert payload["runtime_actor"]["caller_kind"] == "worker"
        assert payload["runtime_actor"]["project_id"] == PROJECT_ID
        orchestrator.main.postgres_db.update_job_status.assert_awaited_once_with(
            job_id=JOB_ID,
            status="processing",
            assigned_agent_id=AGENT_ID,
            expected_status="processing",
        )

    @pytest.mark.asyncio
    async def test_accepted_resume_does_not_resurrect_a_pause_that_won(
        self, resume_collaborators
    ):
        """Flag off: an operator pause landing mid-delivery keeps its row."""
        orchestrator.main.postgres_db.update_job_status.return_value = False

        ok = await control_seams.resume_job_on_agent(_job(), _agent())

        assert ok is False
        orchestrator.main.postgres_db.heartbeat.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_kb_profile_follows_project_scope(self, resume_collaborators):
        await control_seams.resume_job_on_agent(_job(project_id=PROJECT_ID), _agent())
        assert (
            resume_collaborators.injector.await_args.kwargs["include_kb_profile"]
            is True
        )

    @pytest.mark.asyncio
    async def test_non_default_expert_resume_sends_effective_expert_tools(
        self, resume_collaborators, monkeypatch
    ):
        """A generic worker pod must receive the developer tool profile.

        This asserts the effective hydrated config, not merely the presence of
        a wire field: the exact live defect was a syntactically successful
        resume whose effective tools still came from worker_base.
        """
        from shared.runtime.core.loader import (
            get_all_tool_names,
            load_config_from_resolved,
        )

        monkeypatch.setattr(orchestrator.main, "_is_experts_db_enabled", lambda: True)
        monkeypatch.setattr(
            orchestrator.main, "_resolve_default_models", AsyncMock(return_value={})
        )
        patch_service_method(
            monkeypatch,
            expert_catalog_module.ExpertCatalogService,
            "gather_in_scope_skills",
            AsyncMock(return_value=[]),
        )
        monkeypatch.setattr(
            orchestrator.main,
            "_seed_registry_model_overrides",
            AsyncMock(side_effect=lambda config, **_kwargs: config),
        )

        ok = await control_seams.resume_job_on_agent(
            _job(
                config_name="developer",
                config_override={
                    "llm": {"model": "RedHatAI/gemma-4-31B-it-FP8-Dynamic"}
                },
            ),
            _agent(),
        )

        assert ok is True
        payload = _posted_payload()
        assert "config_override" not in payload
        effective = load_config_from_resolved(payload["resolved_config"])
        tool_names = set(get_all_tool_names(effective))
        assert effective.agent_id == "developer"
        assert set(effective.tools.shell) == {
            "run_command",
            "cancel_command",
            "shell_read",
        }
        assert {"run_command", "cancel_command", "shell_read"} <= tool_names
        assert effective.llm.api_key == "sk-llm"
        assert effective.extra["env_keys"] == INJECTED_ENV
        resume_collaborators.injector.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_resolved_resume_grant_denial_still_fails_closed(
        self, resume_collaborators, monkeypatch
    ):
        monkeypatch.setattr(orchestrator.main, "_is_experts_db_enabled", lambda: True)
        monkeypatch.setattr(
            orchestrator.main, "_user_experts_enabled", AsyncMock(return_value=True)
        )
        monkeypatch.setattr(
            orchestrator.main, "_resolve_default_models", AsyncMock(return_value={})
        )
        patch_service_method(
            monkeypatch,
            expert_catalog_module.ExpertCatalogService,
            "gather_in_scope_skills",
            AsyncMock(return_value=[]),
        )
        monkeypatch.setattr(
            orchestrator.main,
            "_seed_registry_model_overrides",
            AsyncMock(side_effect=lambda config, **_kwargs: config),
        )
        monkeypatch.setattr(
            orchestrator.main,
            "_enforce_dispatch_grants",
            AsyncMock(
                side_effect=orchestrator.main.GrantDenied(
                    ["shell_tools: tools.shell requires the shell_tools grant"]
                )
            ),
        )

        ok = await control_seams.resume_job_on_agent(
            _job(config_name="developer"), _agent()
        )

        assert ok is False
        assert _FakeAsyncClient.posts == []
        resume_collaborators.injector.assert_not_awaited()
        orchestrator.main.postgres_db.update_job_status.assert_awaited_once_with(
            JOB_ID,
            status="failed",
            error_message=(
                "config exceeds your capability grants: shell_tools: "
                "tools.shell requires the shell_tools grant"
            ),
        )

    @pytest.mark.asyncio
    async def test_older_agent_keeps_credential_injected_flat_fallback(
        self, resume_collaborators, monkeypatch
    ):
        """A mixed-version rollout must not send a blob the old agent ignores."""
        _FakeAsyncClient.resolved_config_resume = False
        monkeypatch.setattr(orchestrator.main, "_is_experts_db_enabled", lambda: True)
        monkeypatch.setattr(
            orchestrator.main, "_resolve_default_models", AsyncMock(return_value={})
        )
        patch_service_method(
            monkeypatch,
            expert_catalog_module.ExpertCatalogService,
            "gather_in_scope_skills",
            AsyncMock(return_value=[]),
        )
        monkeypatch.setattr(
            orchestrator.main,
            "_seed_registry_model_overrides",
            AsyncMock(side_effect=lambda config, **_kwargs: config),
        )

        ok = await control_seams.resume_job_on_agent(
            _job(config_name="developer"), _agent()
        )

        assert ok is True
        payload = _posted_payload()
        assert "resolved_config" not in payload
        assert payload["config_override"]["llm"]["api_key"] == "sk-llm"
        assert payload["config_override"]["env_keys"] == INJECTED_ENV
        resume_collaborators.injector.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_kb_profile_off_without_project_or_kb_datasource(
        self, resume_collaborators
    ):
        await control_seams.resume_job_on_agent(_job(), _agent())
        assert (
            resume_collaborators.injector.await_args.kwargs["include_kb_profile"]
            is False
        )

    @pytest.mark.asyncio
    async def test_kb_datasource_alone_enables_kb_profile(
        self, resume_collaborators, monkeypatch
    ):
        monkeypatch.setattr(
            orchestrator.main,
            "_resolve_authorized_job_datasources",
            AsyncMock(return_value=[{"type": "kb", "id": "ds-1"}]),
        )
        await control_seams.resume_job_on_agent(_job(), _agent())
        assert (
            resume_collaborators.injector.await_args.kwargs["include_kb_profile"]
            is True
        )

    @pytest.mark.asyncio
    async def test_config_upload_id_passthrough(self, resume_collaborators):
        await control_seams.resume_job_on_agent(
            _job(context={"config_upload_id": "upload-7"}), _agent()
        )
        assert _posted_payload()["config_upload_id"] == "upload-7"

    @pytest.mark.asyncio
    async def test_config_upload_id_absent_stays_off_the_wire(
        self, resume_collaborators
    ):
        await control_seams.resume_job_on_agent(_job(), _agent())
        assert "config_upload_id" not in _posted_payload()

    @pytest.mark.asyncio
    async def test_queued_feedback_delivered_then_cleared(self, resume_collaborators):
        ok = await control_seams.resume_job_on_agent(
            _job(context={"queued_feedback": "fix the tests"}), _agent()
        )

        assert ok is True
        assert _posted_payload()["feedback"] == "fix the tests"
        orchestrator.main.postgres_db.delete_job_context_keys.assert_awaited_once_with(
            JOB_ID, ["queued_feedback"]
        )


class TestResumeJobOnAgentRejection:
    @pytest.mark.asyncio
    async def test_rejection_log_does_not_echo_secret_response(
        self, resume_collaborators, caplog
    ):
        private_material = "-----BEGIN OPENSSH PRIVATE KEY-----hidden"
        _FakeAsyncClient.next_status = 422
        _FakeAsyncClient.next_text = private_material

        assert await control_seams.resume_job_on_agent(_job(), _agent()) is False

        assert private_material not in caplog.text
        assert "status=422" in caplog.text

    @pytest.mark.asyncio
    async def test_409_demotes_stale_ready_agent(self, resume_collaborators):
        _FakeAsyncClient.next_status = 409

        ok = await control_seams.resume_job_on_agent(_job(), _agent())

        assert ok is False
        demote_sql = resume_collaborators.conn.execute.await_args.args[0]
        assert "UPDATE agents SET status = 'working'" in demote_sql
        orchestrator.main.postgres_db.update_job_status.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_other_rejects_do_not_demote(self, resume_collaborators):
        _FakeAsyncClient.next_status = 500

        ok = await control_seams.resume_job_on_agent(_job(), _agent())

        assert ok is False
        resume_collaborators.conn.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_rejected_resume_keeps_queued_feedback(self, resume_collaborators):
        _FakeAsyncClient.next_status = 409

        await control_seams.resume_job_on_agent(
            _job(context={"queued_feedback": "keep me"}), _agent()
        )

        orchestrator.main.postgres_db.delete_job_context_keys.assert_not_awaited()


# ---------------------------------------------------------------------------
# resume_job endpoint — delegation + fallback
# ---------------------------------------------------------------------------


@pytest.fixture
def endpoint_collaborators(monkeypatch, fake_conn):
    """Patch the endpoint's collaborators around the delegation seam."""
    # This fixture covers ordinary Resume delegation with a nontransactional
    # fake connection. Creation-intent Resume has dedicated real-PG coverage.
    monkeypatch.setattr(
        orchestrator.main.postgres_db, "supports_vm_creation_retry", False
    )
    job = _job(assigned_agent_id=AGENT_ID)
    agent = _agent()

    monkeypatch.setattr(
        orchestrator.main,
        "require_internal_or_job_access",
        AsyncMock(return_value=(None, job)),
    )
    monkeypatch.setattr(
        orchestrator.main, "_user_experts_enabled", AsyncMock(return_value=False)
    )
    monkeypatch.setattr(
        orchestrator.main.postgres_db, "get_agent", AsyncMock(return_value=agent)
    )
    monkeypatch.setattr(orchestrator.main.postgres_db, "merge_job_context", AsyncMock())
    claim_job = AsyncMock(return_value=True)
    monkeypatch.setattr(orchestrator.main.postgres_db, "claim_job_for_agent", claim_job)
    queue_for_resume = AsyncMock(return_value=True)
    monkeypatch.setattr(
        orchestrator.main.postgres_db, "queue_job_for_resume", queue_for_resume
    )
    acknowledge_circuit = AsyncMock(return_value=True)
    monkeypatch.setattr(
        orchestrator.main.postgres_db,
        "acknowledge_lease_recovery_circuit",
        acknowledge_circuit,
    )
    queue_stateless = AsyncMock(return_value=True)
    monkeypatch.setattr(
        orchestrator.main.postgres_db, "queue_stateless_job_for_resume", queue_stateless
    )
    prepare_stateless = AsyncMock(return_value=True)
    monkeypatch.setattr(
        orchestrator.main.postgres_db,
        "prepare_stateless_job_for_workspace_resume",
        prepare_stateless,
    )
    monkeypatch.setattr(
        orchestrator.main, "snapshot_service", SimpleNamespace(is_available=False)
    )
    monkeypatch.setattr(orchestrator.main, "_trigger_dispatch", MagicMock())
    delegate = AsyncMock(return_value=True)
    monkeypatch.setattr(
        orchestrator.main,
        "_job_delivery_operations",
        lambda: SimpleNamespace(resume=delegate),
    )
    return SimpleNamespace(
        job=job,
        agent=agent,
        delegate=delegate,
        conn=fake_conn,
        queue_for_resume=queue_for_resume,
        claim_job=claim_job,
        acknowledge_circuit=acknowledge_circuit,
        queue_stateless=queue_stateless,
        prepare_stateless=prepare_stateless,
    )


class TestResumeEndpointDelegation:
    @pytest.mark.asyncio
    async def test_uidless_k8s_runtime_queues_for_live_adoption_without_shedding(
        self, endpoint_collaborators, monkeypatch
    ):
        job = endpoint_collaborators.job
        job["context"]["workspace_container"].update(
            {"provisioner": "k8s", "pod_ip": "10.42.0.17"}
        )
        job["context"]["workspace_container"].pop("_runtime_incarnation")
        prepare = AsyncMock(
            return_value=("wait", job, "kubernetes_attestation_unavailable")
        )
        monkeypatch.setattr(
            orchestrator.main, "_prepare_job_workspace_runtime", prepare
        )
        shed = AsyncMock()
        monkeypatch.setattr(
            orchestrator.main.postgres_db, "shed_workspace_context", shed
        )

        result = await control_seams.resume_job(
            MagicMock(), JOB_ID, orchestrator.main.JobResumeRequest()
        )

        assert result["status"] == "queued"
        endpoint_collaborators.queue_for_resume.assert_awaited_once_with(
            JOB_ID, None, expected_status="paused", lift_operator_pause_hold=""
        )
        endpoint_collaborators.delegate.assert_not_awaited()
        orchestrator.main.postgres_db.get_agent.assert_not_awaited()
        shed.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_stateless_resume_reenqueues_without_registered_agent(
        self, endpoint_collaborators, monkeypatch
    ):
        endpoint_collaborators.job.update(
            {
                "execution_lane": "stateless",
                "status": "pending_review",
                "context": {
                    "workspace_container": {
                        "status": "ready",
                        "provisioner": "k8s",
                        "pod_ip": "10.0.0.8",
                    }
                },
            }
        )
        # This test owns the stateless queue transition, not the historical
        # Kubernetes adoption collaborator exercised in the dedicated cases.
        monkeypatch.setattr(
            orchestrator.main,
            "_prepare_job_workspace_runtime",
            AsyncMock(
                return_value=(
                    "proceed",
                    endpoint_collaborators.job,
                    None,
                )
            ),
        )

        result = await control_seams.resume_job(
            MagicMock(),
            JOB_ID,
            orchestrator.main.JobResumeRequest(feedback="reviewer correction"),
        )

        assert result["status"] == "queued"
        endpoint_collaborators.queue_stateless.assert_awaited_once_with(
            JOB_ID,
            {
                "queued_feedback": "reviewer correction",
                "queued_feedback_reason": (
                    "This job was frozen for review; a reviewer resumed it with "
                    "the feedback below."
                ),
            },
            priority=1,
            fair_key=None,
            expected_status="pending_review",
            lift_operator_pause_hold="",
        )
        endpoint_collaborators.delegate.assert_not_awaited()
        orchestrator.main.postgres_db.get_agent.assert_not_awaited()
        endpoint_collaborators.queue_for_resume.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_fast_path_delegates_to_shared_resume(self, endpoint_collaborators):
        result = await control_seams.resume_job(
            MagicMock(), JOB_ID, orchestrator.main.JobResumeRequest()
        )

        assert result == {
            "status": "resumed",
            "job_id": JOB_ID,
            "agent_id": AGENT_ID,
        }
        endpoint_collaborators.delegate.assert_awaited_once()
        args = endpoint_collaborators.delegate.await_args.args
        assert args[0]["id"] == JOB_ID
        assert args[1] is endpoint_collaborators.agent
        endpoint_collaborators.claim_job.assert_awaited_once_with(
            JOB_ID,
            AGENT_ID,
            allow_failed=True,
            lift_operator_pause_hold="",
        )
        orchestrator.main.postgres_db.merge_job_context.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_feedback_is_merged_and_stamped_on_the_delegated_job(
        self, endpoint_collaborators
    ):
        await control_seams.resume_job(
            MagicMock(),
            JOB_ID,
            orchestrator.main.JobResumeRequest(feedback="try again"),
        )

        # The explicit resume path also stamps the honest [FEEDBACK_RESUME]
        # banner cause (P1-A): a paused job -> the operator wording.
        orchestrator.main.postgres_db.merge_job_context.assert_awaited_once_with(
            JOB_ID,
            {
                "queued_feedback": "try again",
                "queued_feedback_reason": (
                    "An operator explicitly resumed this job with the feedback below."
                ),
            },
        )
        delegated_job = endpoint_collaborators.delegate.await_args.args[0]
        assert delegated_job["context"]["queued_feedback"] == "try again"
        assert delegated_job["context"]["queued_feedback_reason"]

    @pytest.mark.asyncio
    async def test_declined_resume_falls_back_to_queue(self, endpoint_collaborators):
        endpoint_collaborators.delegate.return_value = False

        result = await control_seams.resume_job(
            MagicMock(), JOB_ID, orchestrator.main.JobResumeRequest()
        )

        assert result["status"] == "queued"
        assert result["job_id"] == JOB_ID
        # The re-queue WRITE itself (status flip, agent unassign, freeze shed —
        # the dispatcher-visibility contract) is locked against real Postgres in
        # tests/test_queue_job_for_resume.py. Here we only pin the seam: a
        # declined delegation must fall back to that write.
        endpoint_collaborators.queue_for_resume.assert_awaited_once_with(
            JOB_ID,
            None,
            expected_status="processing",
            lift_operator_pause_hold="",
        )


class TestRedispatchCircuitAcknowledgement:
    @staticmethod
    def _trip(job: dict) -> None:
        job.update(project_id=PROJECT_ID, status="paused")
        job["context"] = {
            "_lease_recovery": {
                "version": 1,
                "generation": "incident-1",
                "state": "tripped",
                "unchanged_recoveries": 3,
            }
        }

    @pytest.mark.asyncio
    async def test_authorized_human_acknowledges_atomically(
        self, endpoint_collaborators
    ):
        self._trip(endpoint_collaborators.job)
        orchestrator.main.require_internal_or_job_access.return_value = (
            {"id": "00000000-0000-0000-0000-0000000000cc"},
            endpoint_collaborators.job,
        )

        result = await control_seams.resume_job(
            MagicMock(),
            JOB_ID,
            orchestrator.main.JobResumeRequest(feedback="retry deliberately"),
        )

        assert result["status"] == "acknowledged"
        endpoint_collaborators.acknowledge_circuit.assert_awaited_once_with(
            JOB_ID,
            expected_status="paused",
            expected_generation="incident-1",
            acknowledged_by={
                "caller_kind": "human",
                "user_id": "00000000-0000-0000-0000-0000000000cc",
            },
            context_merge={
                "queued_feedback": "retry deliberately",
                "queued_feedback_reason": (
                    "An operator explicitly resumed this job with the feedback below."
                ),
            },
            completion_commands_enabled=orchestrator.main.COMPLETION_COMMANDS_ENABLED,
        )
        endpoint_collaborators.delegate.assert_not_awaited()
        endpoint_collaborators.queue_for_resume.assert_not_awaited()
        orchestrator.main._trigger_dispatch.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_current_officer_may_acknowledge_own_project(
        self, endpoint_collaborators, monkeypatch
    ):
        self._trip(endpoint_collaborators.job)
        actor = RuntimeActorContext(
            caller_kind="officer",
            project_id=PROJECT_ID,
            project_role="owner",
            thread_id="00000000-0000-0000-0000-0000000000dd",
            officer_incarnation=4,
        )
        authorize = AsyncMock(return_value=actor)
        monkeypatch.setattr(
            orchestrator.main, "authorize_runtime_actor_request", authorize
        )

        result = await control_seams.resume_job(
            MagicMock(), JOB_ID, orchestrator.main.JobResumeRequest()
        )

        assert result["status"] == "acknowledged"
        authorize.assert_awaited_once_with(
            orchestrator.main.postgres_db,
            ANY,
            action="redispatch_livelock_ack",
            project_id=PROJECT_ID,
        )
        assert (
            endpoint_collaborators.acknowledge_circuit.await_args.kwargs[
                "acknowledged_by"
            ]
            == actor.audit_payload()
        )

    @pytest.mark.asyncio
    async def test_worker_stale_or_foreign_runtime_cannot_acknowledge(
        self, endpoint_collaborators, monkeypatch
    ):
        self._trip(endpoint_collaborators.job)
        authorize = AsyncMock(
            side_effect=HTTPException(
                status_code=403,
                detail={"code": "runtime_not_current"},
            )
        )
        monkeypatch.setattr(
            orchestrator.main, "authorize_runtime_actor_request", authorize
        )

        with pytest.raises(HTTPException) as exc:
            await control_seams.resume_job(
                MagicMock(), JOB_ID, orchestrator.main.JobResumeRequest()
            )

        assert exc.value.status_code == 403
        endpoint_collaborators.acknowledge_circuit.assert_not_awaited()
        endpoint_collaborators.delegate.assert_not_awaited()
        orchestrator.main._trigger_dispatch.assert_not_called()

    @pytest.mark.asyncio
    async def test_acknowledgement_race_reports_no_false_success(
        self, endpoint_collaborators
    ):
        self._trip(endpoint_collaborators.job)
        orchestrator.main.require_internal_or_job_access.return_value = (
            {"id": "00000000-0000-0000-0000-0000000000cc"},
            endpoint_collaborators.job,
        )
        endpoint_collaborators.acknowledge_circuit.return_value = False

        with pytest.raises(HTTPException) as exc:
            await control_seams.resume_job(
                MagicMock(), JOB_ID, orchestrator.main.JobResumeRequest()
            )

        assert exc.value.status_code == 409
        endpoint_collaborators.delegate.assert_not_awaited()
        endpoint_collaborators.queue_for_resume.assert_not_awaited()
        orchestrator.main._trigger_dispatch.assert_not_called()


# ---------------------------------------------------------------------------
# resume_job endpoint — workspaceless jobs go back to the dispatcher
# ---------------------------------------------------------------------------


class TestResumeEndpointWorkspacelessJob:
    """A job whose workspace never came up must be re-provisioned, not pushed.

    Nothing in the resume path provisions; only the dispatcher does. Pushing
    such a job at an agent ships `workspace.backend='vm'` with no `remote`, and
    the agent hard-fails at init_workspace (job 4435994d, 2026-07-27).

    The shed has to happen BEFORE agent selection: the "no agents available"
    branch returns early, so a shed placed after it would leave the parked
    context in place and the dispatcher would just re-park the job.
    """

    @pytest.fixture
    def workspaceless(self, monkeypatch, endpoint_collaborators):
        endpoint_collaborators.job["config_override"] = {"workspace": {"backend": "vm"}}
        endpoint_collaborators.job["context"] = {
            "vm": {"status": "failed", "error": "while scanning a simple key"}
        }
        shed = AsyncMock(return_value=True)
        monkeypatch.setattr(
            orchestrator.main.postgres_db, "shed_workspace_context", shed
        )
        endpoint_collaborators.shed = shed
        return endpoint_collaborators

    @pytest.mark.asyncio
    async def test_queues_instead_of_resuming_onto_an_agent(self, workspaceless):
        result = await control_seams.resume_job(
            MagicMock(), JOB_ID, orchestrator.main.JobResumeRequest()
        )

        assert result["status"] == "queued"
        workspaceless.delegate.assert_not_awaited()
        workspaceless.queue_for_resume.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_sheds_the_parked_vm_context(self, workspaceless):
        """Without this the dispatcher reads status='failed' and re-parks it."""
        await control_seams.resume_job(
            MagicMock(), JOB_ID, orchestrator.main.JobResumeRequest()
        )

        workspaceless.shed.assert_awaited_once_with(JOB_ID, "vm")

    @pytest.mark.asyncio
    async def test_sheds_the_container_key_for_sandbox_jobs(self, workspaceless):
        workspaceless.job["config_override"] = {"workspace": {"backend": "sandbox"}}
        workspaceless.job["context"] = {"workspace_container": {"status": "deleted"}}

        await control_seams.resume_job(
            MagicMock(), JOB_ID, orchestrator.main.JobResumeRequest()
        )

        workspaceless.shed.assert_awaited_once_with(JOB_ID, "workspace_container")

    @pytest.mark.asyncio
    async def test_feedback_survives_the_reprovisioning_detour(self, workspaceless):
        """Re-provisioning must not silently drop what the user typed.

        The pre-flight returns before the endpoint's own feedback merge, so the
        feedback rides along on the queue write instead.
        """
        await control_seams.resume_job(
            MagicMock(),
            JOB_ID,
            orchestrator.main.JobResumeRequest(feedback="try again"),
        )

        workspaceless.queue_for_resume.assert_awaited_once_with(
            JOB_ID,
            {
                "queued_feedback": "try again",
                "queued_feedback_reason": (
                    "An operator explicitly resumed this job with the feedback below."
                ),
            },
            expected_status="paused",
            lift_operator_pause_hold="",
        )

    @pytest.mark.asyncio
    async def test_stateless_reprovision_resume_stamps_intent_without_enqueue(
        self, workspaceless
    ):
        workspaceless.job.update(
            {
                "execution_lane": "stateless",
                "status": "pending_review",
                "config_override": {"workspace": {"backend": "sandbox"}},
                "context": {"workspace_container": {"status": "deleted"}},
            }
        )

        result = await control_seams.resume_job(
            MagicMock(),
            JOB_ID,
            orchestrator.main.JobResumeRequest(feedback="continue after rebuild"),
        )

        assert result["status"] == "queued"
        workspaceless.prepare_stateless.assert_awaited_once_with(
            JOB_ID,
            "workspace_container",
            {
                "queued_feedback": "continue after rebuild",
                "queued_feedback_reason": (
                    "This job was frozen for review; a reviewer resumed it with "
                    "the feedback below."
                ),
            },
            expected_status="pending_review",
            lift_operator_pause_hold="",
        )
        workspaceless.shed.assert_not_awaited()
        workspaceless.queue_for_resume.assert_not_awaited()
        workspaceless.queue_stateless.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_healthy_vm_job_still_resumes_directly(self, workspaceless):
        """Regression: the guard must not divert jobs that have a live VM."""
        workspaceless.job["context"] = {
            "_workspace_contract": {
                "version": 1,
                "requested_backend": "vm",
                "assigned_backend": "vm",
                "assignment_source": "request",
            },
            "vm": {
                "status": "ready",
                "ssh_host": "100.64.0.7",
                "ssh_port": 22,
                "provision_generation": WORKSPACE_RUNTIME,
            },
        }

        result = await control_seams.resume_job(
            MagicMock(), JOB_ID, orchestrator.main.JobResumeRequest()
        )

        assert result["status"] == "resumed"
        workspaceless.delegate.assert_awaited_once()
        workspaceless.shed.assert_not_awaited()


# ---------------------------------------------------------------------------
# resume_job endpoint — Resume PEP grant re-check (decision 9, B3)
# ---------------------------------------------------------------------------
#
# .superpowers/sdd/2026-08-04-expert-write-gate-holes/task-2-brief.md: the PEP
# block re-runs resolve_config + _enforce_dispatch_grants against the
# runner's CURRENT grants before replaying the job, so grants narrowed after
# dispatch are honoured on resume rather than only at dispatch time.
# resolve_config now raises ToolPolicyError DETERMINISTICALLY on a
# permanently-malformed stored `tools` value instead of silently resolving it
# to `[]`. The broad `except Exception` below was written to tolerate
# TRANSIENT errors (a DB blip reading the expert/grant rows) and proceed on
# the assumption that the dispatch-time check still stands; it also caught
# this new deterministic case, turning one bad row into a latch that skips
# the grant check on every resume of every job using that expert, forever,
# while still returning success.


def _capturing_resolve_config(**kwargs):
    """Stand-in for the real ``resolve_config`` (pure/synchronous) that mimics
    its side effect of writing ``merged_fragment`` into the caller's
    ``capture`` dict. ``resume_job`` reads ``_rcap["merged_fragment"]`` when
    building the ``_enforce_dispatch_grants`` call, so a mock that merely
    returns a value and ignores ``capture`` would KeyError on that lookup
    before the collaborator under test in a given case ever runs.
    """
    capture = kwargs.get("capture")
    if capture is not None:
        capture["merged_fragment"] = {}
    return {}


@pytest.fixture
def grant_recheck_collaborators(monkeypatch, endpoint_collaborators):
    """Turn the Resume PEP block ON — ``endpoint_collaborators`` turns it OFF
    (correct for the delegation tests above, which aren't exercising it) —
    and stub its two collaborators, ``resolve_config`` and
    ``_enforce_dispatch_grants``, so each test below controls which one
    raises, and with what.
    """
    monkeypatch.setattr(
        orchestrator.main, "_user_experts_enabled", AsyncMock(return_value=True)
    )
    monkeypatch.setattr(
        orchestrator.main, "_resolve_default_models", AsyncMock(return_value={})
    )
    endpoint_collaborators.resolve_config = MagicMock(
        side_effect=_capturing_resolve_config
    )
    monkeypatch.setattr(
        orchestrator.main, "resolve_config", endpoint_collaborators.resolve_config
    )
    endpoint_collaborators.enforce_dispatch_grants = AsyncMock(return_value=None)
    monkeypatch.setattr(
        orchestrator.main,
        "_enforce_dispatch_grants",
        endpoint_collaborators.enforce_dispatch_grants,
    )
    return endpoint_collaborators


class TestResumeEndpointGrantRecheck:
    @pytest.mark.asyncio
    async def test_malformed_stored_config_fails_closed(
        self, grant_recheck_collaborators
    ):
        """A ToolPolicyError from resolve_config must fail closed and must
        NOT put the job back on an agent. Asserting the status code alone
        would still pass a fix that resumed the job first and merely
        returned the right code afterwards."""
        grant_recheck_collaborators.resolve_config.side_effect = ToolPolicyError(
            "tools.core: unsupported value None (NoneType)."
        )

        with pytest.raises(HTTPException) as ei:
            await control_seams.resume_job(
                MagicMock(), JOB_ID, orchestrator.main.JobResumeRequest()
            )

        assert ei.value.status_code == 409
        # Names the real cause (the stored config, not the caller's grants)
        # and must not read like the 403 GrantDenied branch's wording below.
        assert "cannot be resolved" in ei.value.detail
        assert "your capability grants" not in ei.value.detail
        grant_recheck_collaborators.delegate.assert_not_awaited()
        grant_recheck_collaborators.queue_for_resume.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_transient_resolve_error_still_proceeds(
        self, grant_recheck_collaborators
    ):
        """A plain transient error (DB blip, connection error) must still
        resume the job — the fix is a narrowing of the broad handler, not a
        reversal of the tolerate-and-proceed behaviour it was built for."""
        grant_recheck_collaborators.resolve_config.side_effect = RuntimeError(
            "connection reset by peer"
        )

        result = await control_seams.resume_job(
            MagicMock(), JOB_ID, orchestrator.main.JobResumeRequest()
        )

        assert result == {
            "status": "resumed",
            "job_id": JOB_ID,
            "agent_id": AGENT_ID,
        }
        grant_recheck_collaborators.delegate.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_grant_denied_still_403s(self, grant_recheck_collaborators):
        """Pre-existing behaviour, not previously pinned at the endpoint
        level: an actual grant violation still fails closed with 403, not
        the new 409 this task adds for the unresolvable-config case."""
        grant_recheck_collaborators.enforce_dispatch_grants.side_effect = (
            orchestrator.main.GrantDenied(
                ["shell_tools: tools.shell requires the shell_tools grant"]
            )
        )

        with pytest.raises(HTTPException) as ei:
            await control_seams.resume_job(
                MagicMock(), JOB_ID, orchestrator.main.JobResumeRequest()
            )

        assert ei.value.status_code == 403
        assert "shell_tools" in ei.value.detail
        grant_recheck_collaborators.delegate.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_happy_path_still_resumes(self, grant_recheck_collaborators):
        """Fixture sanity baseline: when both collaborators pass, the PEP
        block is a no-op and the job resumes normally."""
        result = await control_seams.resume_job(
            MagicMock(), JOB_ID, orchestrator.main.JobResumeRequest()
        )

        assert result["status"] == "resumed"
        grant_recheck_collaborators.delegate.assert_awaited_once()

    def test_json_decode_error_is_a_value_error_subclass(self):
        """Pins the assumption the widened `except` relies on rather than
        leaving it for a reader to trust: `json.JSONDecodeError` (raised by
        the two `json.loads` calls below — the job's own `config_override`
        string, and an expert row's stored `config`/`prompts` string) is a
        `ValueError` subclass, so catching `ValueError` covers it without
        naming it. If a future Python/json release ever changed this, this
        test — not a production incident — would be the first to notice."""
        assert issubclass(json.JSONDecodeError, ValueError)

    @pytest.mark.asyncio
    async def test_malformed_config_override_json_fails_closed(
        self, grant_recheck_collaborators
    ):
        """Reviewer-confirmed repro: a job's own `config_override` stored as
        a string that isn't valid JSON raises `json.JSONDecodeError` at
        `_rco = json.loads(_rco)` — BEFORE `resolve_config` is ever called,
        a different line than the other tests in this class exercise, but
        the same try/except, so it must land in the same 409 bucket rather
        than the bare handler that proceeds."""
        grant_recheck_collaborators.job["config_override"] = "{not valid json"

        with pytest.raises(HTTPException) as ei:
            await control_seams.resume_job(
                MagicMock(), JOB_ID, orchestrator.main.JobResumeRequest()
            )

        assert ei.value.status_code == 409
        assert "cannot be resolved" in ei.value.detail
        # Proves *where* it failed, not just that it failed: resolve_config
        # was never reached, because json.loads blew up two lines earlier.
        grant_recheck_collaborators.resolve_config.assert_not_called()
        grant_recheck_collaborators.delegate.assert_not_awaited()
        grant_recheck_collaborators.queue_for_resume.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_unresolvable_config_name_fails_closed(
        self, grant_recheck_collaborators, monkeypatch
    ):
        """Exercises the REAL `resolve_config` (swapping out the fixture's
        stub for this one test only) with a `config_name` that resolves to
        no file anywhere on disk, so this is the actual `FileNotFoundError`
        `load_and_merge_config` raises (src/core/loader.py's unguarded
        `open()` at the top of the function, hit because `resolve_config`'s
        own internal $extends probe swallows the first attempt and retries
        against the same missing path) — not a mocked stand-in for it.
        """
        from orchestrator.services.config_resolver import (
            resolve_config as real_resolve_config,
        )

        monkeypatch.setattr(orchestrator.main, "resolve_config", real_resolve_config)
        grant_recheck_collaborators.job["config_name"] = (
            "this-config-name-does-not-exist-anywhere-2f9c1a"
        )

        with pytest.raises(HTTPException) as ei:
            await control_seams.resume_job(
                MagicMock(), JOB_ID, orchestrator.main.JobResumeRequest()
            )

        assert ei.value.status_code == 409
        assert "cannot be resolved" in ei.value.detail
        grant_recheck_collaborators.delegate.assert_not_awaited()
        grant_recheck_collaborators.queue_for_resume.assert_not_awaited()


class TestResumePayloadGitRemote:
    """The resume payload must carry the job repo remote.

    Without it the agent's pod-handoff clone fallback
    (``metadata["git_remote_url"]`` gate in ``_setup_job_workspace``) can
    never fire, so a resume onto a fresh workspace with no snapshot silently
    restarts from a blank tree
    (knowledge-base/knowledge/issues/resume_fresh_workspace_no_clone_fallback.md).
    """

    @pytest.mark.asyncio
    async def test_resume_payload_carries_git_remote_for_pod_handoff(
        self, resume_collaborators
    ):
        job = _job(context={"git_remote_url": "http://srw-gitea:3000/srw/job-1.git"})

        assert await control_seams.resume_job_on_agent(job, _agent()) is True

        assert (
            _posted_payload()["git_remote_url"] == "http://srw-gitea:3000/srw/job-1.git"
        )

    @pytest.mark.asyncio
    async def test_resume_payload_omits_git_remote_when_job_has_none(
        self, resume_collaborators
    ):
        assert await control_seams.resume_job_on_agent(_job(), _agent()) is True

        assert "git_remote_url" not in _posted_payload()
