"""What a refused resume does with the workspace it keeps (decision 34)."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from orchestrator.services import managed_repository_process_retirement
from orchestrator.services import refused_workspace_credentials as subject

JOB_ID = "00000000-0000-0000-0000-0000000000c1"
PARENT_ID = "00000000-0000-0000-0000-0000000000c0"
FINGERPRINT = "SHA256:" + "f" * 43
TARGET = subject.ScrubTarget("10.0.0.9", 30022, FINGERPRINT)


def _job(backend: str = "sandbox", **context) -> dict:
    return {
        "id": JOB_ID,
        "config_override": {"workspace": {"backend": backend}},
        "context": {
            "_workspace_contract": {
                "version": 1,
                "requested_backend": backend,
                "assigned_backend": backend,
                "assignment_source": "test",
            },
            **context,
        },
    }


class TestPlan:
    @pytest.mark.parametrize("reason", sorted(subject.REVOCATION_REASONS))
    def test_a_revocation_keeps_the_workspace_and_scrubs_it(self, reason):
        assert subject.plan_refused_resume(
            reason, job=_job(), target=TARGET
        ) == subject.RefusedResume(keep_workspace=True, scrub=TARGET)

    @pytest.mark.parametrize(
        "reason",
        [
            "model_unavailable",
            "unrouted_model",
            "workspace_contract",
            "lite_config",
            "lite_shell_connector",
            "missing_ssh_remote",
        ],
    )
    def test_any_other_refusal_keeps_everything(self, reason):
        assert subject.plan_refused_resume(
            reason, job=_job(), target=TARGET
        ) == subject.RefusedResume(keep_workspace=True)

    def test_a_revocation_no_endpoint_reaches_is_torn_down(self):
        assert subject.plan_refused_resume(
            "grant_denied", job=_job(), target=None
        ) == subject.RefusedResume(keep_workspace=False)

    @pytest.mark.parametrize("backend", ["virtual", "none"])
    def test_a_lite_tier_has_nothing_to_scrub(self, backend):
        assert subject.plan_refused_resume(
            "connector_unavailable", job=_job(backend), target=None
        ) == subject.RefusedResume(keep_workspace=True, skipped="no_shell_workspace")

    @pytest.mark.parametrize(
        "extra",
        [
            {
                "parent_job_id": PARENT_ID,
                "context": {"inherits_parent_workspace": True},
            },
            {"context": {"provisions_parent_workspace": PARENT_ID}},
        ],
        ids=["inherits", "provisions"],
    )
    def test_a_child_on_its_parents_workspace_leaves_the_parents_material(self, extra):
        job = _job()
        job.update({key: value for key, value in extra.items() if key != "context"})
        job["context"].update(extra["context"])

        assert subject.plan_refused_resume(
            "connector_unavailable", job=job, target=TARGET
        ) == subject.RefusedResume(keep_workspace=True, skipped="shared_workspace")


class TestTargets:
    def test_an_attestation_names_its_endpoint_and_pin(self):
        attestation = SimpleNamespace(
            host="10.0.0.9",
            pod_ip="10.0.0.9",
            port=30022,
            ssh_host_key_fingerprint=FINGERPRINT,
        )
        assert subject.attested_scrub_target(attestation) == TARGET

    @pytest.mark.parametrize(
        "attestation",
        [
            None,
            SimpleNamespace(host="h", pod_ip="h", port=22, ssh_host_key_fingerprint=""),
            SimpleNamespace(
                host="", pod_ip="", port=22, ssh_host_key_fingerprint=FINGERPRINT
            ),
        ],
    )
    def test_an_incomplete_attestation_names_nothing(self, attestation):
        assert subject.attested_scrub_target(attestation) is None

    def test_a_probe_authenticated_vm_names_its_pin(self):
        job = _job(
            "vm",
            vm={
                "status": "ready",
                "ssh_ready_source": "provisioner_probe",
                "identity_authenticated": True,
                "identity_provision_generation": "g1",
                "provision_generation": "g1",
                "ssh_host": "100.64.0.5",
                "ssh_port": 22,
                "ssh_host_key_fingerprint": FINGERPRINT,
            },
        )
        assert subject.vm_scrub_target(job) == subject.ScrubTarget(
            "100.64.0.5", 22, FINGERPRINT
        )

    @pytest.mark.parametrize(
        "change",
        [
            {"identity_authenticated": False},
            {"identity_provision_generation": "g0"},
            {"ssh_ready_source": "agent"},
            {"status": "failed"},
            {"ssh_host_key_fingerprint": None},
        ],
    )
    def test_an_unauthenticated_vm_names_nothing(self, change):
        vm = {
            "status": "ready",
            "ssh_ready_source": "provisioner_probe",
            "identity_authenticated": True,
            "identity_provision_generation": "g1",
            "provision_generation": "g1",
            "ssh_host": "100.64.0.5",
            "ssh_port": 22,
            "ssh_host_key_fingerprint": FINGERPRINT,
            **change,
        }
        assert subject.vm_scrub_target(_job("vm", vm=vm)) is None


class TestScrub:
    @pytest.fixture
    def scrub(self, monkeypatch):
        run = AsyncMock(return_value=True)
        monkeypatch.setattr(
            managed_repository_process_retirement, "scrub_workspace_credentials", run
        )
        return run

    @pytest.mark.asyncio
    async def test_the_scrub_runs_on_the_pinned_endpoint_and_is_recorded(self, scrub):
        store = SimpleNamespace(merge_job_context=AsyncMock())

        await subject.scrub_refused_workspace(
            store,
            JOB_ID,
            reason="grant_denied",
            plan=subject.RefusedResume(keep_workspace=True, scrub=TARGET),
        )

        scrub.assert_awaited_once_with(
            host="10.0.0.9", port=30022, host_key_fingerprint=FINGERPRINT
        )
        store.merge_job_context.assert_awaited_once_with(
            JOB_ID,
            {
                subject.SCRUB_CONTEXT_KEY: {
                    "reason": "grant_denied",
                    "outcome": "scrubbed",
                }
            },
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "failure", [False, RuntimeError("ssh down")], ids=["failed", "raised"]
    )
    async def test_a_failed_scrub_is_recorded_and_never_raised(
        self, monkeypatch, failure
    ):
        run = AsyncMock(
            side_effect=failure if isinstance(failure, Exception) else None,
            return_value=failure,
        )
        monkeypatch.setattr(
            managed_repository_process_retirement, "scrub_workspace_credentials", run
        )
        store = SimpleNamespace(merge_job_context=AsyncMock())

        await subject.scrub_refused_workspace(
            store,
            JOB_ID,
            reason="connector_unavailable",
            plan=subject.RefusedResume(keep_workspace=True, scrub=TARGET),
        )

        record = store.merge_job_context.await_args.args[1][subject.SCRUB_CONTEXT_KEY]
        assert record == {"reason": "connector_unavailable", "outcome": "failed"}

    @pytest.mark.asyncio
    async def test_an_unrecordable_outcome_is_only_logged(self, scrub):
        store = SimpleNamespace(merge_job_context=AsyncMock(side_effect=OSError))

        await subject.scrub_refused_workspace(
            store,
            JOB_ID,
            reason="connector_unavailable",
            plan=subject.RefusedResume(keep_workspace=True, scrub=TARGET),
        )

        scrub.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_a_skipped_scrub_is_recorded_without_running(self, scrub):
        store = SimpleNamespace(merge_job_context=AsyncMock())

        await subject.scrub_refused_workspace(
            store,
            JOB_ID,
            reason="connector_unavailable",
            plan=subject.RefusedResume(keep_workspace=True, skipped="shared_workspace"),
        )

        scrub.assert_not_awaited()
        store.merge_job_context.assert_awaited_once_with(
            JOB_ID,
            {
                subject.SCRUB_CONTEXT_KEY: {
                    "reason": "connector_unavailable",
                    "outcome": "skipped",
                    "detail": "shared_workspace",
                }
            },
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "plan",
        [
            subject.RefusedResume(keep_workspace=True),
            subject.RefusedResume(keep_workspace=False),
        ],
        ids=["kept", "torn-down"],
    )
    async def test_nothing_to_scrub_does_nothing(self, scrub, plan):
        store = SimpleNamespace(merge_job_context=AsyncMock())

        await subject.scrub_refused_workspace(
            store, JOB_ID, reason="model_unavailable", plan=plan
        )

        scrub.assert_not_awaited()
        store.merge_job_context.assert_not_awaited()
