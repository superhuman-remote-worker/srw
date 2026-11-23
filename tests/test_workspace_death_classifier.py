"""How a job's workspace ended, and what its hold says about it.

The orchestrator reads the ``workspace`` container's termination from the
exact dead Pod (a pure extractor), keeps it on the job row, and a strict
classifier calls it a memory kill only on the kubelet's ``OOMKilled``. A bare
exit 137 is also what a liveness kill or a lost runtime event leaves, so it is
"not recorded". The hold then says the workspace ran out of memory, with the
limit, and what the next Resume does: double the limit once, up to the
installation's cap, or point to a new job.
knowledge-base/knowledge/issues/workspace_out_of_memory_is_reported_as_transport_lost.md
"""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from orchestrator.services.container_provisioner import workspace_termination_from_pod
from shared.workspace_disk import (
    MORE_MEMORY_HINT,
    RESUME_REQUIRED,
    WORKSPACE_OUT_OF_MEMORY_REASON,
    WorkspaceMemoryGrowth,
    WorkspaceMemoryKill,
    WorkspaceTermination,
    byte_quantity,
    decide_memory_growth,
    new_memory_growth_record,
    recorded_termination,
    workspace_hold_resume_reason,
    workspace_memory_attention,
)

GIB = 1 << 30
MIB = 1 << 20
UID = "6f0d6c9e-4c55-4a4b-9a43-0b1d7b0a1111"
OTHER_UID = "6f0d6c9e-4c55-4a4b-9a43-0b1d7b0a2222"


def _pod(
    *,
    reason="OOMKilled",
    exit_code=137,
    finished_at=datetime(2026, 11, 10, 20, 18, 50, tzinfo=timezone.utc),
    deletion_timestamp=None,
    pod_reason=None,
    memory="4Gi",
    terminated=True,
    uid=UID,
):
    state = SimpleNamespace(
        terminated=(
            SimpleNamespace(reason=reason, exit_code=exit_code, finished_at=finished_at)
            if terminated
            else None
        ),
        running=None if terminated else SimpleNamespace(),
    )
    return SimpleNamespace(
        metadata=SimpleNamespace(uid=uid, deletion_timestamp=deletion_timestamp),
        status=SimpleNamespace(
            phase="Failed" if terminated else "Running",
            reason=pod_reason,
            message=None,
            container_statuses=[
                SimpleNamespace(name="ssh-sidecar", state=SimpleNamespace()),
                SimpleNamespace(name="workspace", state=state),
            ],
        ),
        spec=SimpleNamespace(
            containers=[
                SimpleNamespace(
                    name="workspace",
                    resources=SimpleNamespace(
                        limits={"memory": memory, "cpu": "2"} if memory else {}
                    ),
                )
            ]
        ),
    )


class TestExtractor:
    def test_reads_the_workspace_container_of_the_exact_pod(self):
        record = workspace_termination_from_pod(_pod())
        assert record == WorkspaceTermination(
            pod_uid=UID,
            container_reason="OOMKilled",
            exit_code=137,
            finished_at="2026-11-10T20:18:50+00:00",
            memory_limit_bytes=4 * GIB,
        )

    def test_a_running_container_has_no_record(self):
        assert workspace_termination_from_pod(_pod(terminated=False)) is None

    def test_a_pod_without_uid_has_no_record(self):
        assert workspace_termination_from_pod(_pod(uid=None)) is None

    def test_the_limit_is_the_pods_own(self):
        assert (
            workspace_termination_from_pod(_pod(memory="512Mi")).memory_limit_bytes
            == 512 * MIB
        )
        assert (
            workspace_termination_from_pod(_pod(memory=None)).memory_limit_bytes is None
        )
        assert (
            workspace_termination_from_pod(_pod(memory="nonsense")).memory_limit_bytes
            is None
        )

    def test_keeps_the_deletion_timestamp(self):
        deleted = datetime(2026, 11, 10, 20, 18, 40, tzinfo=timezone.utc)
        record = workspace_termination_from_pod(_pod(deletion_timestamp=deleted))
        assert record.deletion_timestamp == "2026-11-10T20:18:40+00:00"


class TestStrictClassifier:
    def test_oomkilled_is_out_of_memory(self):
        assert workspace_termination_from_pod(_pod()).cause == "out_of_memory"

    @pytest.mark.parametrize(
        ("reason", "exit_code"),
        [
            ("Error", 137),
            ("ContainerStatusUnknown", 137),
            ("Unknown", 255),
            ("Error", 1),
            ("Completed", 0),
            (None, 137),
        ],
    )
    def test_anything_else_is_not_recorded(self, reason, exit_code):
        record = workspace_termination_from_pod(
            _pod(reason=reason, exit_code=exit_code)
        )
        assert record.cause == "not_recorded"
        # The raw facts stay on the record.
        assert record.exit_code == exit_code
        assert record.container_reason == reason

    def test_evicted_is_its_own_cause(self):
        assert (
            workspace_termination_from_pod(
                _pod(reason="Error", pod_reason="Evicted")
            ).cause
            == "evicted"
        )
        assert workspace_termination_from_pod(_pod(pod_reason="Evicted")).cause == (
            "evicted"
        )

    def test_a_container_srw_stopped_is_never_a_memory_kill(self):
        # Deleted at :40, stopped at :50: SRW's delete stopped it.
        record = workspace_termination_from_pod(
            _pod(
                deletion_timestamp=datetime(
                    2026, 11, 10, 20, 18, 40, tzinfo=timezone.utc
                )
            )
        )
        assert record.cause == "not_recorded"

    def test_killed_before_its_pod_was_deleted_stays_a_memory_kill(self):
        record = workspace_termination_from_pod(
            _pod(
                deletion_timestamp=datetime(
                    2026, 11, 10, 20, 19, 0, tzinfo=timezone.utc
                )
            )
        )
        assert record.cause == "out_of_memory"

    def test_deleted_without_a_finish_time_proves_nothing(self):
        record = workspace_termination_from_pod(
            _pod(
                finished_at=None,
                deletion_timestamp=datetime(2026, 11, 10, tzinfo=timezone.utc),
            )
        )
        assert record.cause == "not_recorded"

    def test_missing_or_malformed_record_is_absent(self):
        assert WorkspaceTermination.from_dict(None) is None
        assert WorkspaceTermination.from_dict({}) is None
        assert WorkspaceTermination.from_dict({"pod_uid": 7}) is None
        record = WorkspaceTermination.from_dict(
            {"pod_uid": UID, "exit_code": "137", "memory_limit_bytes": -1}
        )
        assert record.exit_code is None
        assert record.memory_limit_bytes is None

    def test_record_round_trips(self):
        record = workspace_termination_from_pod(_pod())
        assert WorkspaceTermination.from_dict(record.as_dict()) == record

    def test_a_reader_uses_the_record_only_for_its_own_runtime(self):
        workspace = {
            "last_termination": workspace_termination_from_pod(_pod()).as_dict()
        }
        assert recorded_termination(workspace, UID) is not None
        assert recorded_termination(workspace, OTHER_UID) is None
        assert recorded_termination({}, UID) is None


class TestGrowth:
    def test_doubles_once(self):
        growth = decide_memory_growth(
            limit_bytes=4 * GIB,
            previous=None,
            cap_bytes=16 * GIB,
            runtime_incarnation=UID,
        )
        assert growth == WorkspaceMemoryGrowth("granted", 8 * GIB)

    def test_never_above_the_cap(self):
        assert decide_memory_growth(
            limit_bytes=12 * GIB,
            previous=None,
            cap_bytes=16 * GIB,
            runtime_incarnation=UID,
        ) == WorkspaceMemoryGrowth("granted", 16 * GIB)
        assert decide_memory_growth(
            limit_bytes=16 * GIB,
            previous=None,
            cap_bytes=16 * GIB,
            runtime_incarnation=UID,
        ) == WorkspaceMemoryGrowth("capped")

    def test_off_without_a_cap_or_a_limit(self):
        assert decide_memory_growth(
            limit_bytes=4 * GIB, previous=None, cap_bytes=None, runtime_incarnation=UID
        ) == WorkspaceMemoryGrowth("off")
        assert decide_memory_growth(
            limit_bytes=None, previous=None, cap_bytes=16 * GIB, runtime_incarnation=UID
        ) == WorkspaceMemoryGrowth("off")

    def test_a_second_kill_finds_it_used(self):
        previous = new_memory_growth_record(
            WorkspaceMemoryGrowth("granted", 8 * GIB),
            from_bytes=4 * GIB,
            runtime_incarnation=UID,
            at="2026-11-20T10:00:00+00:00",
            previous=None,
        )
        assert previous == {
            "limit": "8Gi",
            "limit_bytes": 8 * GIB,
            "from_bytes": 4 * GIB,
            "runtime_incarnation": UID,
            "at": "2026-11-20T10:00:00+00:00",
        }
        # The same kill read twice (hold, then cleanup) stays granted.
        assert decide_memory_growth(
            limit_bytes=4 * GIB,
            previous=previous,
            cap_bytes=16 * GIB,
            runtime_incarnation=UID,
        ) == WorkspaceMemoryGrowth("granted", 8 * GIB)
        # The next runtime's kill does not grow again.
        assert decide_memory_growth(
            limit_bytes=8 * GIB,
            previous=previous,
            cap_bytes=16 * GIB,
            runtime_incarnation=OTHER_UID,
        ) == WorkspaceMemoryGrowth("used", 8 * GIB)
        # Nor does it write a second record.
        assert (
            new_memory_growth_record(
                WorkspaceMemoryGrowth("granted", 8 * GIB),
                from_bytes=4 * GIB,
                runtime_incarnation=UID,
                at="later",
                previous=previous,
            )
            is None
        )

    def test_a_refused_growth_stays_spent(self):
        previous = {
            "limit_bytes": 8 * GIB,
            "runtime_incarnation": UID,
            "refused_at": "2026-11-20T10:01:00+00:00",
        }
        assert decide_memory_growth(
            limit_bytes=4 * GIB,
            previous=previous,
            cap_bytes=16 * GIB,
            runtime_incarnation=OTHER_UID,
        ) == WorkspaceMemoryGrowth("refused", 8 * GIB)

    @pytest.mark.parametrize(
        ("value", "quantity"),
        [(8 * GIB, "8Gi"), (768 * MIB, "768Mi"), (3 * 1024, "3Ki"), (1000, "1000")],
    )
    def test_byte_quantity(self, value, quantity):
        assert byte_quantity(value) == quantity


def _kill(outcome, *, limit=4 * GIB, grown=8 * GIB):
    return WorkspaceMemoryKill(
        limit_bytes=limit,
        growth=WorkspaceMemoryGrowth(outcome, grown if outcome != "off" else None),
        exit_code=137,
        container_reason="OOMKilled",
    )


class TestWording:
    def test_first_kill_says_what_resume_does(self):
        assert _kill("granted").action() == (
            "workspace ran out of memory (limit 4 GiB) and was stopped; interrupted "
            "command outcome is unknown; explicit Resume required. Resume raises the "
            "job's memory limit to 8 GiB."
        )

    def test_second_kill_gives_the_hint(self):
        assert _kill("used", limit=8 * GIB).action() == (
            "workspace ran out of memory again (limit 8 GiB, already raised once) and "
            "was stopped; interrupted command outcome is unknown; explicit Resume "
            "required. " + MORE_MEMORY_HINT
        )

    @pytest.mark.parametrize("outcome", ["capped", "off", "refused"])
    def test_no_growth_left_gives_the_hint(self, outcome):
        text = _kill(outcome).action()
        assert text.startswith("workspace ran out of memory (limit 4 GiB) and was")
        assert text.endswith(MORE_MEMORY_HINT)
        assert "raises" not in text

    def test_an_unknown_limit_is_not_invented(self):
        text = _kill("off", limit=None).action()
        assert text.startswith("workspace ran out of memory and was stopped;")

    def test_never_says_transport_lost(self):
        for outcome in ("granted", "used", "capped", "off", "refused"):
            assert "transport" not in _kill(outcome).action()

    def test_the_list_cut_keeps_the_action(self):
        assert RESUME_REQUIRED in _kill("granted").action()[:180]

    def test_attention_round_trips(self):
        kill = _kill("granted")
        attention = workspace_memory_attention(kill)
        assert attention["reason"] == WORKSPACE_OUT_OF_MEMORY_REASON
        assert WorkspaceMemoryKill.from_attention(attention) == kill
        assert WorkspaceMemoryKill.from_attention({"reason": "other"}) is None
        assert WorkspaceMemoryKill.from_attention(None) is None


class TestResumeReason:
    def _hold(self, attention, source="workspace_recovery_unavailable"):
        return {"source": source, "attention": attention}

    def test_memory_kill_tells_the_agent_its_new_limit_and_what_survived(self):
        attention = {
            "reason": WORKSPACE_OUT_OF_MEMORY_REASON,
            "runtime_replaced": True,
            "volume_kept": True,
            **workspace_memory_attention(_kill("granted")),
        }
        assert workspace_hold_resume_reason(self._hold(attention)) == (
            "This job's workspace ran out of memory (limit 4 GiB) and was stopped; "
            "the outcome of the command that was running is unknown. Its memory "
            "limit is now 8 GiB. Files under /home/agent-host are kept; running "
            "processes and other paths such as /tmp are gone. The job's saved "
            "progress is kept. An operator resumed it with the feedback below."
        )

    def test_without_growth_the_limit_stays(self):
        attention = workspace_memory_attention(_kill("capped"))
        reason = workspace_hold_resume_reason(self._hold(attention))
        assert "Its memory limit stays 4 GiB." in reason

    def test_a_lost_workspace_without_a_recorded_cause(self):
        reason = workspace_hold_resume_reason(
            self._hold({"reason": "interrupted_command_outcome_unknown"})
        )
        assert reason.startswith("This job's workspace stopped answering;")
        assert "memory" not in reason

    def test_other_holds_keep_the_callers_wording(self):
        assert workspace_hold_resume_reason({"source": "operator_pause"}) is None
        assert workspace_hold_resume_reason(None) is None


class TestJobSurfaces:
    """``get_job`` and ``list_jobs`` render a memory hold from its attention."""

    def _job(self, kill, *, phase="settled"):
        from shared.workspace_disk import workspace_kept_clause

        kept = workspace_kept_clause(runtime_replaced=True, volume_kept=True)
        return kept, {
            "id": "11111111-2222-3333-4444-555555555555",
            "status": "paused",
            "error_message": kill.action() + " " + kept,
            "context": {
                "_operator_pause_hold": {
                    "source": "workspace_recovery_unavailable",
                    "paused_by": None,
                    "attention": {
                        "runtime_replaced": True,
                        "volume_kept": True,
                        **workspace_memory_attention(kill),
                    },
                },
                "workspace_container": {"recovery_cleanup": {"phase": phase}},
            },
        }

    def test_get_job_names_the_memory_kill_and_the_next_resume(self):
        from shared.orch_surface.formatters import format_job_detail

        kept, job = self._job(_kill("granted"))
        detail = format_job_detail(job)
        assert detail.count(kept) == 1
        assert detail.splitlines()[-2:] == [
            "Workspace recovery hold: workspace ran out of memory (limit 4 GiB) and "
            "was stopped; interrupted command outcome is unknown; explicit Resume "
            "required. Resume raises the job's memory limit to 8 GiB. " + kept,
            "Error: workspace ran out of memory (limit 4 GiB) and was stopped; "
            "interrupted command outcome is unknown; explicit Resume required. "
            "Resume raises the job's memory limit to 8 GiB.",
        ]
        assert "transport" not in detail

    def test_pending_cleanup_still_announces_the_growth(self):
        from shared.orch_surface.formatters import format_job_detail

        _, job = self._job(_kill("granted"), phase="pending")
        assert (
            "unknown; cleanup must settle before Resume. Resume raises the job's "
            "memory limit to 8 GiB."
        ) in format_job_detail(job)

    def test_second_kill_gives_the_hint(self):
        from shared.orch_surface.formatters import format_job_detail

        _, job = self._job(_kill("used", limit=8 * GIB))
        assert MORE_MEMORY_HINT in format_job_detail(job)

    def test_list_cut_keeps_the_action(self):
        from shared.orch_surface.formatters import format_jobs

        for outcome in ("granted", "used", "capped"):
            _, job = self._job(_kill(outcome))
            (error_line,) = [
                line for line in format_jobs([job]).splitlines() if "Error:" in line
            ]
            assert "ran out of memory" in error_line
            assert "explicit Resume required" in error_line
