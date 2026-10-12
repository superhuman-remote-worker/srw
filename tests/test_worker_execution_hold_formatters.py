import pytest

from shared.orch_surface.formatters import format_job_detail


@pytest.mark.parametrize("marker", [None, False, {}, {"phase": "pending"}])
def test_pending_executor_hold_never_invites_ordinary_resume(marker):
    text = format_job_detail(
        {
            "id": "held-job",
            "status": "paused",
            "context": {
                "_worker_execution_hold": marker,
                "_operator_pause_hold": {"source": "worker_execution_outcome_unknown"},
            },
        }
    )
    assert "command outcome is unknown" in text
    assert "Resume is blocked" in text
    assert "until an explicit resume" not in text
