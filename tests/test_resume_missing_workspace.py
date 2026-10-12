"""Resume must not ship a workspace-backed job that has no workspace address.

``_resume_job_on_agent`` injects ``workspace.remote`` only when the job's
context says the workspace is already live — ``context.vm.status == 'ready'``
with an ``ssh_host``, or ``context.workspace_container.status == 'ready'`` with
a host. Neither block has an else. When the workspace never came up (or was
reaped), injection is silently skipped while the job's own config_override
still says ``workspace.backend='vm'``, so the agent receives a backend with no
host to dial and dies at ``init_workspace`` (src/agent.py:1896) with a
misleading "no workspace.remote config was provided".

Nothing in the resume path provisions anything — only the dispatcher does — so
such a job must be queued for dispatch instead of pushed at an agent. That is
what job 4435994d hit: VM provisioning failed, Resume was clicked, and the
resume burned an agent and overwrote the original diagnostic error.

Mirrors the plain-unit style of tests/test_resume_stale_agent_requeue.py.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, patch

import pytest

from tests import _b09_control_seams as control_seams

VM_JOB = {"config_override": {"workspace": {"backend": "vm"}}}
SANDBOX_JOB = {"config_override": {"workspace": {"backend": "sandbox"}}}

RUNTIME_ID = "00000000-0000-0000-0000-000000000901"
READY_VM = {
    "status": "ready",
    "ssh_host": "100.64.0.7",
    "ssh_port": 22,
    "provision_generation": RUNTIME_ID,
}
READY_CONTAINER = {
    "status": "ready",
    "host": "workspace-abc.svc",
    "port": 22,
    "_runtime_incarnation": RUNTIME_ID,
}


def _job(base: dict, **context) -> dict:
    return {**base, "context": context}


class TestResumeMissingWorkspace:
    """Which workspace a resume would ship without an address, if any."""

    def test_vm_job_with_no_vm_context(self):
        assert control_seams.resume_missing_workspace(_job(VM_JOB)) == "vm"

    def test_vm_job_whose_provisioning_failed(self):
        """The job 4435994d shape: context.vm holds an error, never an ssh_host."""

        job = _job(
            VM_JOB, vm={"status": "failed", "error": "while scanning a simple key"}
        )
        assert control_seams.resume_missing_workspace(job) == "vm"

    def test_vm_job_ready_but_without_ssh_host(self):
        job = _job(VM_JOB, vm={"status": "ready"})
        assert control_seams.resume_missing_workspace(job) == "vm"

    def test_healthy_vm_job_is_resumable(self):
        assert control_seams.resume_missing_workspace(_job(VM_JOB, vm=READY_VM)) is None

    def test_sandbox_job_with_no_container(self):
        assert control_seams.resume_missing_workspace(_job(SANDBOX_JOB)) == "sandbox"

    def test_sandbox_job_whose_container_was_reaped(self):
        job = _job(SANDBOX_JOB, workspace_container={"status": "deleted"})
        assert control_seams.resume_missing_workspace(job) == "sandbox"

    def test_sandbox_job_ready_but_without_host(self):
        job = _job(SANDBOX_JOB, workspace_container={"status": "ready"})
        assert control_seams.resume_missing_workspace(job) == "sandbox"

    def test_healthy_sandbox_job_is_resumable(self):
        job = _job(SANDBOX_JOB, workspace_container=READY_CONTAINER)
        assert control_seams.resume_missing_workspace(job) is None

    def test_healthy_sandbox_job_via_pod_ip_is_resumable(self):
        """The container block accepts pod_ip as a fallback for host."""

        job = _job(
            SANDBOX_JOB,
            workspace_container={
                "status": "ready",
                "pod_ip": "10.1.2.3",
                "_runtime_incarnation": RUNTIME_ID,
            },
        )
        assert control_seams.resume_missing_workspace(job) is None

    @pytest.mark.parametrize("backend", ["virtual", "none"])
    def test_lite_tiers_need_no_workspace(self, backend):
        """virtual/none run with no workspace pod at all — never block them."""

        job = _job({"config_override": {"workspace": {"backend": backend}}})
        assert control_seams.resume_missing_workspace(job) is None

    def test_context_may_arrive_as_a_json_string(self):
        """asyncpg hands context back as raw JSON; the helpers must cope."""

        job = {**VM_JOB, "context": json.dumps({"vm": READY_VM})}
        assert control_seams.resume_missing_workspace(job) is None

    def test_config_override_may_arrive_as_a_json_string(self):
        job = {
            "config_override": json.dumps({"workspace": {"backend": "vm"}}),
            "context": {},
        }
        assert control_seams.resume_missing_workspace(job) == "vm"


class TestResumeJobOnAgentRefusesWorkspacelessJob:
    """The guard must fire before any DB or HTTP work is done."""

    @pytest.mark.asyncio
    async def test_returns_false_without_doing_any_work(self):
        """False is _resume_job_on_agent's 'caller should queue' contract.

        Asserting the return value alone would pass vacuously — the function
        already returns False for a workspaceless job today, because its first
        DB call throws into the broad except. So this pins the thing that
        actually changes: the guard fires BEFORE any work, leaving the job's
        datasources (and everything downstream of them) untouched.
        """
        import orchestrator.main as om

        job = {
            "id": "4435994d-b029-444d-8a3c-26c64abd456a",
            "config_override": {"workspace": {"backend": "vm"}},
            "context": {"vm": {"status": "failed", "error": "boom"}},
        }
        agent = {"id": "c5651fce-c097-419c-822e-ada39c8c9d43", "pod_ip": "10.42.0.9"}

        resolve = AsyncMock(return_value=[])
        shed = AsyncMock(return_value=True)
        with (
            patch.object(
                om.app.state.resources.postgres_db,
                "resolve_datasources_for_job",
                resolve,
            ),
            patch.object(
                om.app.state.resources.postgres_db, "shed_workspace_context", shed
            ),
        ):
            assert await control_seams.resume_job_on_agent(job, agent) is False

        resolve.assert_not_called()
        # Sheds the parked context so the dispatcher re-provisions rather than
        # re-parking — otherwise the dispatcher's own resume path would bounce
        # this job every tick without ever rebuilding the workspace.
        shed.assert_awaited_once_with(job["id"], "vm")

    @pytest.mark.asyncio
    async def test_still_refuses_when_the_shed_fails(self):
        """A shed error must not escape — callers expect a bool, not a raise.

        The guard runs outside this function's try/except, and the dispatcher's
        call site does not wrap it, so a DB hiccup here would otherwise take out
        the dispatch loop.
        """
        import orchestrator.main as om

        job = {
            "id": "4435994d-b029-444d-8a3c-26c64abd456a",
            "config_override": {"workspace": {"backend": "vm"}},
            "context": {"vm": {"status": "failed"}},
        }
        agent = {"id": "c5651fce-c097-419c-822e-ada39c8c9d43", "pod_ip": "10.42.0.9"}

        shed = AsyncMock(side_effect=RuntimeError("Not connected to database"))
        with patch.object(
            om.app.state.resources.postgres_db, "shed_workspace_context", shed
        ):
            assert await control_seams.resume_job_on_agent(job, agent) is False

    @pytest.mark.asyncio
    async def test_flag_on_refusal_does_not_shed_after_dispatch_claim(self):
        import orchestrator.main as om

        job = {
            "id": "4435994d-b029-444d-8a3c-26c64abd456a",
            "config_override": {"workspace": {"backend": "vm"}},
            "context": {"vm": {"status": "failed"}},
        }
        agent = {
            "id": "c5651fce-c097-419c-822e-ada39c8c9d43",
            "pod_ip": "10.42.0.9",
        }
        shed = AsyncMock()
        with (
            patch.object(
                om.app.state.resources.settings, "completion_commands_enabled", True
            ),
            patch.object(
                om.app.state.resources.postgres_db, "shed_workspace_context", shed
            ),
        ):
            assert await control_seams.resume_job_on_agent(job, agent) is False

        shed.assert_not_awaited()
