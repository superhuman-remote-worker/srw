"""The optional cap on model calls, ``limits.max_llm_calls_per_job``.

It is counted as ``iteration - llm_call_baseline`` on checkpointed graph state
and checked at the top of the execute node, before compaction and before the
request is built. So, unlike the tool-call budget (a closure of the tool node,
which restarts with every rebuilt graph), it holds across a stateless batch
rotation. At the cap the job stops with a non-recoverable ``error`` that names
the limit and the count; the orchestrator settles that as ``failed``.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from agent.graph import llm_call_limit_stop
from agent.tools.core.job import _final_phase_data, _missing_deliverable_refused
from shared.runtime.core.loader import (
    LimitsConfig,
    load_agent_config,
    load_agent_config_from_dict,
    load_config_from_resolved,
    resolve_config_path,
    serialize_resolved_config,
)
from tests._agent_mode_harness import (
    BATCH_REENTRY,
    batch_arm,
    build_job,
    job_complete_call,
    write_file_call,
)

REPO_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(autouse=True)
def _clean_process_caches():
    _final_phase_data.clear()
    _missing_deliverable_refused.clear()
    yield
    _final_phase_data.clear()
    _missing_deliverable_refused.clear()


def _writes(count: int, prefix: str = "w") -> list:
    return [write_file_call(f"{prefix}{n}", path=f"f{n}.txt") for n in range(count)]


class _Config:
    def __init__(self, **limits):
        self.limits = LimitsConfig(**limits)


class TestStopAtTheCap:
    @pytest.mark.asyncio
    async def test_the_job_stops_before_the_call_over_the_cap(self, tmp_path):
        # The model would go on writing files; the cap allows two calls.
        job = build_job(tmp_path, _writes(5), limits={"max_llm_calls_per_job": 2})

        out = await job.start()

        assert len(job.requests) == 2
        error = out["error"]
        assert error["type"] == "llm_call_limit"
        assert error["recoverable"] is False
        assert (error["limit"], error["count"]) == (2, 2)
        assert "max_llm_calls_per_job" in error["message"]
        assert "2 model calls" in error["message"]
        # Routed to END: the report envelope carries the error.
        assert out["should_stop"] is True
        assert out["completion_report_payload"]["error"]["type"] == "llm_call_limit"
        assert out.get("freeze_data") is None
        # No call was made by the stop: iteration stays, the baseline moves.
        assert out["iteration"] == 2
        assert out["llm_call_baseline"] == 2
        assert out["plain_reply_streak"] == 0
        assert out["todos"][0]["status"] != "completed"

    @pytest.mark.asyncio
    async def test_the_cap_counts_across_a_batch_rotation(self, tmp_path):
        """Run to the cap minus one, end the batch, build a new graph on the
        same checkpoint: one more call reaches the cap and the next execute
        stops. A count kept in a closure would have restarted at zero."""
        cap = 3
        job = build_job(
            tmp_path, _writes(cap - 1, "a"), limits={"max_llm_calls_per_job": cap}
        )

        first = await job.start(**batch_arm(0, cap - 1))

        assert len(job.requests) == cap - 1
        assert first["freeze_data"]["freeze_type"] == "batch_boundary"
        assert first["freeze_data"]["boundary"] == "mid_phase"
        assert first["iteration"] == cap - 1

        # The next claim: a new graph, a new model, the same checkpoint.
        job.rebuild(_writes(5, "b"))
        out = await job.resume({**batch_arm(first["iteration"], 100), **BATCH_REENTRY})

        assert len(job.llms[-1].requests) == 1
        assert len(job.requests) == cap
        assert out["error"]["type"] == "llm_call_limit"
        assert out["error"]["count"] == cap
        assert out["should_stop"] is True

    @pytest.mark.asyncio
    async def test_zero_disables_the_cap(self, tmp_path):
        job = build_job(
            tmp_path,
            [*_writes(6), job_complete_call()],
            limits={"max_llm_calls_per_job": 0},
        )

        out = await job.start()

        assert len(job.requests) == 7
        assert out.get("error") is None
        assert out["goal_achieved"] is True


class TestLimitStopFunction:
    def test_off_by_default(self):
        assert llm_call_limit_stop({"iteration": 10_000}, _Config()) is None

    def test_old_checkpoint_without_baseline_counts_from_zero(self):
        state = {"iteration": 4}  # no llm_call_baseline key
        assert llm_call_limit_stop(state, _Config(max_llm_calls_per_job=5)) is None
        stop = llm_call_limit_stop({"iteration": 5}, _Config(max_llm_calls_per_job=5))
        assert stop["error"]["count"] == 5
        assert stop["llm_call_baseline"] == 5

    def test_the_baseline_starts_a_fresh_budget(self):
        config = _Config(max_llm_calls_per_job=3)
        assert (
            llm_call_limit_stop({"iteration": 7, "llm_call_baseline": 5}, config)
            is None
        )
        stop = llm_call_limit_stop({"iteration": 8, "llm_call_baseline": 5}, config)
        assert stop["error"]["count"] == 3
        assert "iteration" not in stop  # no call was made

    def test_a_non_integer_limit_is_off(self):
        config = _Config()
        config.limits.max_llm_calls_per_job = "5"
        assert llm_call_limit_stop({"iteration": 50}, config) is None


class TestConfiguration:
    def test_the_worker_overlay_states_both_limits_off(self):
        overlay = yaml.safe_load(
            (REPO_ROOT / "config" / "overlays" / "worker.yaml").read_text()
        )
        assert overlay["limits"]["max_llm_calls_per_job"] == 0
        assert overlay["limits"]["max_plain_replies_in_a_row"] == 0

        config = load_agent_config(*resolve_config_path("worker_base"))
        assert config.limits.max_llm_calls_per_job == 0
        assert config.limits.max_plain_replies_in_a_row == 0

    def test_the_dataclass_defaults_are_off(self):
        limits = LimitsConfig()
        assert limits.max_llm_calls_per_job == 0
        assert limits.max_plain_replies_in_a_row == 0

    def test_both_parse_sites_keep_the_keys(self, tmp_path):
        limits = {"max_llm_calls_per_job": 40, "max_plain_replies_in_a_row": 3}
        from_dict = load_agent_config_from_dict(
            {"agent_id": "probe", "display_name": "Probe", "limits": limits}
        )
        leaf = tmp_path / "probe.yaml"
        leaf.write_text(
            yaml.safe_dump(
                {
                    "$extends": "worker_base",
                    "agent_id": "probe",
                    "display_name": "Probe",
                    "limits": limits,
                }
            )
        )
        from_file = load_agent_config(str(leaf))
        for loaded in (from_dict, from_file):
            assert loaded.limits.max_llm_calls_per_job == 40
            assert loaded.limits.max_plain_replies_in_a_row == 3

    def test_the_limits_round_trip_through_the_resolved_blob(self):
        config = load_agent_config(*resolve_config_path("worker_base"))
        config.limits.max_llm_calls_per_job = 100
        config.limits.max_plain_replies_in_a_row = 3

        blob = serialize_resolved_config(config, model=config.llm.model)
        assert blob["agent"]["limits"]["max_llm_calls_per_job"] == 100
        assert blob["agent"]["limits"]["max_plain_replies_in_a_row"] == 3

        hydrated = load_config_from_resolved(blob)
        assert hydrated.limits.max_llm_calls_per_job == 100
        assert hydrated.limits.max_plain_replies_in_a_row == 3
