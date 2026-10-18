"""Tests for AuxHealth — observability for silent auxiliary-task failures.

See knowledge-base/knowledge/issues/surface_silent_aux_failures.md.
"""

import logging

from shared.runtime.services.auxiliary import AuxHealth


class TestAuxHealth:
    def test_starts_healthy(self):
        h = AuxHealth(model="gemma-4-moe")
        assert h.degraded is False
        snap = h.snapshot()
        assert snap["model"] == "gemma-4-moe"
        assert snap["degraded"] is False
        assert snap["consecutive_failures"] == 0
        assert snap["tasks"] == {}

    def test_below_threshold_does_not_escalate(self, caplog):
        h = AuxHealth()
        with caplog.at_level(logging.ERROR):
            h.record_failure("memory_extraction", RuntimeError("boom"))
            h.record_failure("memory_extraction", RuntimeError("boom"))
        assert h.degraded is False
        assert not [r for r in caplog.records if "DEGRADED" in r.getMessage()]

    def test_escalates_after_three_consecutive(self, caplog):
        h = AuxHealth(model="gemma-4-moe")
        with caplog.at_level(logging.ERROR):
            for _ in range(3):
                h.record_failure("memory_extraction", ConnectionError("503"))
        assert h.degraded is True
        degraded = [
            r for r in caplog.records if "AUXILIARY MODEL DEGRADED" in r.getMessage()
        ]
        assert len(degraded) == 1
        assert "gemma-4-moe" in degraded[0].getMessage()

    def test_repeat_is_throttled_then_reemits(self, caplog):
        h = AuxHealth()
        with caplog.at_level(logging.ERROR):
            for _ in range(AuxHealth.ESCALATE_AFTER + AuxHealth.REPEAT_EVERY):
                h.record_failure("title_generation", TimeoutError())
        # One at the threshold (n=3), one re-emit at n=3+REPEAT_EVERY.
        msgs = [r for r in caplog.records if "DEGRADED" in r.getMessage()]
        assert len(msgs) == 2

    def test_recovery_resets_and_logs(self, caplog):
        h = AuxHealth()
        for _ in range(3):
            h.record_failure("memory_extraction", ConnectionError())
        assert h.degraded is True
        with caplog.at_level(logging.ERROR):
            h.record_success("memory_extraction")
        assert h.degraded is False
        assert h.snapshot()["consecutive_failures"] == 0
        assert [r for r in caplog.records if "RECOVERED" in r.getMessage()]

    def test_success_on_any_task_clears_consecutive(self):
        h = AuxHealth()
        h.record_failure("memory_extraction", ConnectionError())
        h.record_failure("memory_extraction", ConnectionError())
        # A success anywhere means the model is reachable -> not degraded.
        h.record_success("title_generation")
        snap = h.snapshot()
        assert snap["consecutive_failures"] == 0
        # Per-task history is retained for diagnostics.
        assert snap["tasks"]["memory_extraction"]["failures"] == 2
        assert snap["tasks"]["memory_extraction"]["consecutive_failures"] == 2

    def test_snapshot_records_last_error(self):
        h = AuxHealth()
        h.record_failure("knowledge_curation", ValueError("bad json"))
        t = h.snapshot()["tasks"]["knowledge_curation"]
        assert t["failures"] == 1
        assert t["consecutive_failures"] == 1
        assert t["last_error_type"] == "ValueError"
        assert "bad json" in t["last_error"]
        assert t["last_failure_at"] is not None


class TestAuxHeartbeatSummary:
    """The compact projection carried on the agent → orchestrator heartbeat.

    Drives the persisted ``agents.aux_degraded`` flag + the admin badge tooltip
    (aux Phase 2). Always includes ``degraded`` so a recovered agent clears it.
    """

    def test_healthy_summary(self):
        h = AuxHealth(model="gemma-4-moe")
        assert h.heartbeat_summary() == {
            "degraded": False,
            "consecutive_failures": 0,
            "model": "gemma-4-moe",
            "failing_tasks": {},
            "on_fallback": False,
            "last_fallback_error": None,
        }

    def test_degraded_lists_only_failing_tasks(self):
        h = AuxHealth(model="gemma-4-moe")
        h.record_success("title_generation")  # healthy task — must not appear
        for _ in range(3):
            h.record_failure("memory_extraction", ConnectionError("503"))
        s = h.heartbeat_summary()
        assert s["degraded"] is True
        assert s["consecutive_failures"] == 3
        assert s["model"] == "gemma-4-moe"
        assert set(s["failing_tasks"]) == {"memory_extraction"}
        assert s["failing_tasks"]["memory_extraction"] == {
            "consecutive_failures": 3,
            "last_error_type": "ConnectionError",
        }

    def test_recovery_clears_failing_tasks(self):
        h = AuxHealth()
        for _ in range(3):
            h.record_failure("memory_extraction", ConnectionError())
        assert h.heartbeat_summary()["degraded"] is True
        h.record_success("memory_extraction")
        s = h.heartbeat_summary()
        assert s["degraded"] is False
        assert s["consecutive_failures"] == 0
        assert s["failing_tasks"] == {}
