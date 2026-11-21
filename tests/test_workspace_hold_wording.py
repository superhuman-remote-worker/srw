"""What a workspace hold says was kept and what is gone.

One clause per state, from two facts the producers record on the hold
(``runtime_replaced``, ``volume_kept``); the formatter renders the same clause
from those facts and ``get_job`` prints it once.
knowledge-base/knowledge/issues/workspace_loss_pause_text_and_liveness_time_mislead.md
"""

from __future__ import annotations

import pytest

from shared.orch_surface.formatters import format_job_detail, format_jobs
from shared.workspace_disk import (
    RESUME_REQUIRED,
    recorded_workspace_kept_clause,
    volume_kept_from_backing,
    workspace_hold_facts,
    workspace_kept_clause,
)

SAVED = "The job's saved progress is kept."


class TestFacts:
    @pytest.mark.parametrize(
        ("backing_id", "kept"),
        [
            ("k8s-pvc:ns:uid", True),
            ("k8s-pod:ns:uid", False),
            ("vm:ns:uid", None),
            ("", None),
            (None, None),
            (42, None),
        ],
    )
    def test_volume_from_backing_prefix(self, backing_id, kept):
        assert volume_kept_from_backing(backing_id) is kept

    def test_facts_read_the_binding(self):
        assert workspace_hold_facts(
            runtime_replaced=True, binding={"backing_id": "k8s-pvc:ns:uid"}
        ) == {"runtime_replaced": True, "volume_kept": True}
        assert workspace_hold_facts(runtime_replaced=None, binding="garbage") == {
            "runtime_replaced": None,
            "volume_kept": None,
        }


class TestClause:
    def test_pod_gone_volume_kept(self):
        assert workspace_kept_clause(runtime_replaced=True, volume_kept=True) == (
            "Files under /home/agent-host are kept; running processes and other "
            "paths such as /tmp are gone. " + SAVED
        )

    def test_pod_gone_no_volume(self):
        # What is gone, and what a Resume gives back.
        assert workspace_kept_clause(runtime_replaced=True, volume_kept=False) == (
            "The workspace had no persistent volume, so files that were not pushed "
            "to the job repository are gone; Resume starts a new workspace and "
            "clones the job repository into it again. The job's saved progress is "
            "kept."
        )

    def test_pod_still_running_claims_nothing_lost(self):
        for volume in (True, False, None):
            assert workspace_kept_clause(
                runtime_replaced=False, volume_kept=volume
            ) == (
                "The workspace is still up and nothing was removed; its processes "
                "and /tmp last until it is released."
            )

    def test_unchecked_pod_never_claims_it_runs(self):
        for volume in (True, False, None):
            clause = workspace_kept_clause(runtime_replaced=None, volume_kept=volume)
            assert clause.startswith("Nothing was removed")
            assert "not known" in clause
            assert "still up" not in clause
            assert clause.endswith(SAVED)
        assert "/home/agent-host are kept" in workspace_kept_clause(
            runtime_replaced=None, volume_kept=True
        )

    def test_no_clause_says_checkpoints_or_storage_retained(self):
        for replaced in (True, False, None):
            for volume in (True, False, None):
                clause = workspace_kept_clause(
                    runtime_replaced=replaced, volume_kept=volume
                )
                assert "checkpoint" not in clause
                assert "retained" not in clause

    def test_malformed_facts_count_as_unknown(self):
        assert workspace_kept_clause(
            runtime_replaced="yes", volume_kept=1
        ) == workspace_kept_clause(runtime_replaced=None, volume_kept=None)

    def test_hold_written_before_the_facts_says_only_what_is_always_true(self):
        assert recorded_workspace_kept_clause(None) == SAVED
        assert recorded_workspace_kept_clause({"reason": "x"}) == SAVED
        assert recorded_workspace_kept_clause(
            {"runtime_replaced": True, "volume_kept": True}
        ) == workspace_kept_clause(runtime_replaced=True, volume_kept=True)


def _held(source, attention, error_message, **context):
    return {
        "id": "11111111-2222-3333-4444-555555555555",
        "status": "paused",
        "error_message": error_message,
        "context": {
            "_operator_pause_hold": {
                "source": source,
                "paused_by": None,
                "attention": attention,
            },
            **context,
        },
    }


class TestJobSurfaces:
    def test_get_job_says_what_was_kept_once(self):
        kept = workspace_kept_clause(runtime_replaced=True, volume_kept=True)
        job = _held(
            "workspace_recovery_unavailable",
            {"runtime_replaced": True, "volume_kept": True},
            "workspace transport lost; interrupted command outcome is unknown; "
            f"explicit Resume required. {kept}",
            workspace_container={"recovery_cleanup": {"phase": "settled"}},
        )
        detail = format_job_detail(job)
        assert detail.count(kept) == 1
        assert detail.splitlines()[-2:] == [
            "Workspace recovery hold: interrupted command outcome is unknown; "
            f"explicit Resume required. {kept}",
            "Error: workspace transport lost; interrupted command outcome is "
            "unknown; explicit Resume required.",
        ]

    def test_pending_cleanup_gate_and_live_pod(self):
        live = _held(
            "workspace_recovery_unavailable",
            {"runtime_replaced": False, "volume_kept": True},
            None,
        )
        assert (
            "Workspace recovery hold: interrupted command outcome is unknown; "
            "explicit Resume required. The workspace is still up and nothing was "
            "removed; its processes and /tmp last until it is released."
        ) in format_job_detail(live)
        pending = _held(
            "workspace_recovery_unavailable",
            {"runtime_replaced": True, "volume_kept": True},
            None,
            workspace_container={"recovery_cleanup": {"phase": "pending"}},
        )
        assert "; cleanup must settle before Resume. Files under" in (
            format_job_detail(pending)
        )

    def test_an_unrelated_error_message_is_left_whole(self):
        job = _held(
            "workspace_recovery_exhausted",
            {"runtime_replaced": None, "volume_kept": True},
            "something else entirely",
        )
        assert "Error: something else entirely" in format_job_detail(job)

    @pytest.mark.parametrize(
        "cause",
        [
            "workspace transport lost; interrupted command outcome is unknown; ",
            "worker reported exhausted workspace retries; ",
            "workspace recovery exhausted after 3 attempts; ",
        ],
    )
    def test_list_cut_keeps_the_action(self, cause):
        """list_jobs cuts the error at 180 characters; the action comes first."""
        for replaced in (True, False, None):
            for volume in (True, False, None):
                message = (
                    cause
                    + RESUME_REQUIRED
                    + ". "
                    + workspace_kept_clause(
                        runtime_replaced=replaced, volume_kept=volume
                    )
                )
                job = {"id": "j", "status": "paused", "error_message": message}
                (error_line,) = [
                    line for line in format_jobs([job]).splitlines() if "Error:" in line
                ]
                assert "explicit Resume required" in error_line
