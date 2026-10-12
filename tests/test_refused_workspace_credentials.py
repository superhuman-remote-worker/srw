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
READY_VM = {
    "status": "ready",
    "ssh_ready_source": "provisioner_probe",
    "identity_authenticated": True,
    "identity_provision_generation": "g1",
    "provision_generation": "g1",
    "ssh_host": "100.64.0.5",
    "ssh_port": 22,
    "ssh_host_key_fingerprint": FINGERPRINT,
}


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
            reason, job=_job(), target=subject.Unreachable("anything")
        ) == subject.RefusedResume(keep_workspace=True)

    def test_a_revocation_nothing_reaches_is_torn_down_and_says_why(self):
        assert subject.plan_refused_resume(
            "grant_denied",
            job=_job(),
            target=subject.Unreachable("no attested workspace endpoint"),
        ) == subject.RefusedResume(
            keep_workspace=False, unreachable="no attested workspace endpoint"
        )

    @pytest.mark.parametrize("backend", ["virtual", "none"])
    def test_a_lite_tier_has_nothing_to_scrub(self, backend):
        assert subject.plan_refused_resume(
            "connector_unavailable",
            job=_job(backend),
            target=subject.Unreachable("no attested workspace endpoint"),
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
    def test_an_attestation_names_its_endpoint_pin_and_shell_fence(self):
        attestation = SimpleNamespace(
            host="10.0.0.9",
            pod_ip="10.0.0.9",
            port=30022,
            ssh_host_key_fingerprint=FINGERPRINT,
            workspace_generation="gen-1",
            runtime_incarnation="pod-1",
        )

        assert subject.attested_scrub_target(
            attestation, shell_owner_token=7
        ) == subject.ScrubTarget(
            "10.0.0.9",
            30022,
            FINGERPRINT,
            workspace_generation="gen-1",
            runtime_incarnation="pod-1",
            shell_owner_token=7,
        )

    @pytest.mark.parametrize(
        ("attestation", "why"),
        [
            (None, "no attested workspace endpoint"),
            (
                SimpleNamespace(
                    host="h", pod_ip="h", port=22, ssh_host_key_fingerprint=""
                ),
                "the attestation names no host key",
            ),
            (
                SimpleNamespace(
                    host="", pod_ip="", port=22, ssh_host_key_fingerprint=FINGERPRINT
                ),
                "the attestation names no endpoint",
            ),
        ],
    )
    def test_an_incomplete_attestation_says_why(self, attestation, why):
        assert subject.attested_scrub_target(attestation) == subject.Unreachable(why)

    def test_a_probe_authenticated_same_cluster_vm_names_its_pin(self, monkeypatch):
        monkeypatch.setenv("VM_MODE", "same-cluster")

        assert subject.vm_scrub_target(
            _job("vm", vm=dict(READY_VM))
        ) == subject.ScrubTarget("100.64.0.5", 22, FINGERPRINT)

    @pytest.mark.parametrize(
        ("change", "why"),
        [
            (
                {"identity_authenticated": False},
                "no provisioner probe authenticated the VM's host key",
            ),
            (
                {"identity_provision_generation": "g0"},
                "no provisioner probe authenticated the VM's host key",
            ),
            (
                {"ssh_ready_source": "agent"},
                "no provisioner probe authenticated the VM's host key",
            ),
            ({"status": "failed"}, "the VM is not ready"),
            ({"ssh_host_key_fingerprint": None}, "the VM names no host key"),
        ],
    )
    def test_an_unauthenticated_vm_says_why(self, monkeypatch, change, why):
        monkeypatch.setenv("VM_MODE", "same-cluster")

        assert subject.vm_scrub_target(
            _job("vm", vm={**READY_VM, **change})
        ) == subject.Unreachable(why)

    @pytest.mark.parametrize("mode", ["external", "nats"])
    def test_a_vm_outside_same_cluster_mode_is_unreachable(self, monkeypatch, mode):
        monkeypatch.setenv("VM_MODE", mode)

        assert isinstance(
            subject.vm_scrub_target(_job("vm", vm=dict(READY_VM))),
            subject.Unreachable,
        )

    def test_a_job_without_a_vm_has_no_attested_endpoint(self):
        assert subject.vm_scrub_target(_job()) == subject.Unreachable(
            "no attested workspace endpoint"
        )


class TestShellRetirement:
    @pytest.fixture
    def backends(self, monkeypatch):
        from shared.runtime.core.backends import remote

        built = []

        class Backend:
            def __init__(self, **kwargs):
                self.kwargs = kwargs
                self.calls = []
                built.append(self)

            def set_shell_owner_token(self, token):
                self.calls.append(("token", token))

            def shell_cleanup(self):
                self.calls.append(("cleanup",))

            def shell_cleanup_strict(self):
                self.calls.append(("cleanup_strict",))

            def retire(self):
                self.calls.append(("retire",))

        monkeypatch.setattr(remote, "RemoteBackend", Backend)
        monkeypatch.setattr(subject, "resolve_ssh_key_path", lambda: "/test/key")
        return built

    @pytest.mark.asyncio
    async def test_a_pinned_shell_is_retired_with_an_acknowledgement(self, backends):
        assert await subject.retire_job_shells(JOB_ID, TARGET) is True

        [backend] = backends
        assert backend.kwargs["job_id"] == JOB_ID
        assert backend.kwargs["host"] == "10.0.0.9"
        assert backend.kwargs["expected_host_key_fingerprint"] == FINGERPRINT
        assert backend.kwargs["require_host_key_fingerprint"] is True
        assert backend.calls == [("cleanup_strict",), ("retire",)]

    @pytest.mark.asyncio
    async def test_a_stateless_shell_is_retired_under_the_claims_lease(self, backends):
        target = subject.ScrubTarget(
            "10.0.0.9",
            30022,
            FINGERPRINT,
            workspace_generation="gen-1",
            runtime_incarnation="pod-1",
            shell_owner_token=7,
        )

        assert await subject.retire_job_shells(JOB_ID, target) is True

        [backend] = backends
        assert backend.kwargs["workspace_generation"] == "gen-1"
        assert backend.kwargs["runtime_incarnation"] == "pod-1"
        assert backend.calls == [("token", 7), ("cleanup",), ("retire",)]

    @pytest.mark.asyncio
    async def test_an_unacknowledged_retirement_is_false(self, backends, monkeypatch):
        from shared.runtime.core.backends import remote

        class Refusing(remote.RemoteBackend):
            def shell_cleanup_strict(self):
                raise RuntimeError("not acknowledged")

        monkeypatch.setattr(remote, "RemoteBackend", Refusing)

        assert await subject.retire_job_shells(JOB_ID, TARGET) is False
        assert backends[-1].calls == [("retire",)]


class TestScrub:
    @pytest.fixture
    def scrub(self, monkeypatch):
        files = AsyncMock(return_value=True)
        shells = AsyncMock(return_value=True)
        monkeypatch.setattr(
            managed_repository_process_retirement, "scrub_workspace_credentials", files
        )
        monkeypatch.setattr(subject, "retire_job_shells", shells)
        return SimpleNamespace(files=files, shells=shells)

    @pytest.mark.asyncio
    async def test_the_scrub_ends_the_shells_scrubs_the_files_and_is_recorded(
        self, scrub
    ):
        store = SimpleNamespace(merge_job_context=AsyncMock())

        await subject.scrub_refused_workspace(
            store,
            JOB_ID,
            reason="grant_denied",
            plan=subject.RefusedResume(keep_workspace=True, scrub=TARGET),
        )

        scrub.shells.assert_awaited_once_with(JOB_ID, TARGET)
        scrub.files.assert_awaited_once_with(
            host="10.0.0.9", port=30022, host_key_fingerprint=FINGERPRINT
        )
        store.merge_job_context.assert_awaited_once_with(
            JOB_ID,
            {
                subject.SCRUB_CONTEXT_KEY: {
                    "refusal": "grant_denied",
                    "outcome": "scrubbed",
                    "shells": "retired",
                    "files": "scrubbed",
                    "remains": list(subject.SCRUB_REMAINS),
                }
            },
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("shells", "files", "expected"),
        [
            (True, False, {"shells": "retired", "files": "failed"}),
            (False, True, {"shells": "not_retired", "files": "scrubbed"}),
            (
                RuntimeError("ssh down"),
                RuntimeError("ssh down"),
                {"shells": "not_retired", "files": "failed"},
            ),
        ],
        ids=["files", "shells", "raised"],
    )
    async def test_a_partial_scrub_is_recorded_as_failed_and_never_raised(
        self, scrub, shells, files, expected
    ):
        for mock, result in ((scrub.shells, shells), (scrub.files, files)):
            if isinstance(result, Exception):
                mock.side_effect = result
            else:
                mock.return_value = result
        store = SimpleNamespace(merge_job_context=AsyncMock())

        await subject.scrub_refused_workspace(
            store,
            JOB_ID,
            reason="connector_unavailable",
            plan=subject.RefusedResume(keep_workspace=True, scrub=TARGET),
        )

        record = store.merge_job_context.await_args.args[1][subject.SCRUB_CONTEXT_KEY]
        assert record["outcome"] == "failed"
        assert {key: record[key] for key in expected} == expected

    @pytest.mark.asyncio
    async def test_an_unrecordable_outcome_is_only_logged(self, scrub):
        store = SimpleNamespace(merge_job_context=AsyncMock(side_effect=OSError))

        await subject.scrub_refused_workspace(
            store,
            JOB_ID,
            reason="connector_unavailable",
            plan=subject.RefusedResume(keep_workspace=True, scrub=TARGET),
        )

        scrub.files.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_a_skipped_scrub_is_recorded_without_running(self, scrub):
        store = SimpleNamespace(merge_job_context=AsyncMock())

        await subject.scrub_refused_workspace(
            store,
            JOB_ID,
            reason="connector_unavailable",
            plan=subject.RefusedResume(keep_workspace=True, skipped="shared_workspace"),
        )

        scrub.shells.assert_not_awaited()
        scrub.files.assert_not_awaited()
        store.merge_job_context.assert_awaited_once_with(
            JOB_ID,
            {
                subject.SCRUB_CONTEXT_KEY: {
                    "refusal": "connector_unavailable",
                    "outcome": "skipped",
                    "reason": "shared_workspace",
                }
            },
        )

    @pytest.mark.asyncio
    async def test_a_torn_down_workspace_records_why(self, scrub):
        store = SimpleNamespace(merge_job_context=AsyncMock())

        await subject.scrub_refused_workspace(
            store,
            JOB_ID,
            reason="grant_denied",
            plan=subject.RefusedResume(
                keep_workspace=False, unreachable="VM mode external"
            ),
        )

        scrub.files.assert_not_awaited()
        store.merge_job_context.assert_awaited_once_with(
            JOB_ID,
            {
                subject.SCRUB_CONTEXT_KEY: {
                    "refusal": "grant_denied",
                    "outcome": "torn_down",
                    "reason": "unreachable: VM mode external",
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
        ids=["kept", "fresh-start"],
    )
    async def test_nothing_to_scrub_records_nothing(self, scrub, plan):
        store = SimpleNamespace(merge_job_context=AsyncMock())

        await subject.scrub_refused_workspace(
            store, JOB_ID, reason="model_unavailable", plan=plan
        )

        scrub.files.assert_not_awaited()
        store.merge_job_context.assert_not_awaited()
