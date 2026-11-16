"""The job's mode at admission: the request field, its checks and sub-jobs.

``job_mode`` on ``JobCreate`` is folded into ``config_override`` before the
config stage reads it, so the resolver freezes it with the rest of the job's
configuration. Unknown values and contradictions are refused, as is agent mode
on a live-session job. Sub-jobs (the verification critic, the scholar pass)
always state ``worker`` and never inherit a parent's or a default's mode.
"""

from __future__ import annotations

import copy
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException
from pydantic import ValidationError

from orchestrator.schemas.job_create import PUBLIC_JOB_CREATE_FIELDS, JobCreate
from orchestrator.services import job_admission
from orchestrator.services.job_admission import (
    JobAdmissionDependencies,
    admit_job,
    admit_job_mode,
)


class TestTheRequestField:
    def test_it_is_public_and_optional(self):
        assert "job_mode" in PUBLIC_JOB_CREATE_FIELDS
        assert JobCreate(description="x").job_mode is None
        assert JobCreate(description="x", job_mode="agent").job_mode == "agent"
        assert JobCreate(description="x", job_mode="worker").job_mode == "worker"

    def test_an_unknown_value_is_refused_by_the_schema(self):
        with pytest.raises(ValidationError):
            JobCreate(description="x", job_mode="light")

    def test_the_public_schema_documents_the_two_values(self):
        from orchestrator.schemas.job_create import public_job_create_schema

        field = public_job_create_schema()["properties"]["job_mode"]
        values = {
            value
            for branch in field.get("anyOf", [field])
            for value in branch.get("enum", ())
        }
        assert values == {"worker", "agent"}
        assert "agent" in field["description"] and "worker" in field["description"]


class TestAdmitJobMode:
    def test_the_field_is_folded_into_the_override(self):
        command = JobCreate(
            description="x",
            job_mode="agent",
            config_override={"autonomy": "full", "llm": {"model": "m"}},
        )
        admit_job_mode(command)
        assert command.config_override == {
            "autonomy": "full",
            "llm": {"model": "m"},
            "job_mode": "agent",
        }

    def test_the_field_alone_creates_the_override(self):
        command = JobCreate(description="x", job_mode="worker")
        admit_job_mode(command)
        assert command.config_override == {"job_mode": "worker"}

    def test_without_the_field_nothing_changes(self):
        command = JobCreate(description="x", config_override={"autonomy": "full"})
        admit_job_mode(command)
        assert command.config_override == {"autonomy": "full"}
        command = JobCreate(description="x")
        admit_job_mode(command)
        assert command.config_override is None

    @pytest.mark.parametrize("mode", ["agent", "worker"])
    def test_a_valid_override_value_passes(self, mode):
        command = JobCreate(description="x", config_override={"job_mode": mode})
        admit_job_mode(command)
        assert command.config_override == {"job_mode": mode}

    @pytest.mark.parametrize("value", ["light", "Agent", 1, True, ["agent"]])
    def test_an_unknown_override_value_is_refused(self, value):
        command = JobCreate(description="x", config_override={"job_mode": value})
        with pytest.raises(HTTPException) as exc:
            admit_job_mode(command)
        assert exc.value.status_code == 400
        assert "config_override.job_mode" in exc.value.detail

    def test_a_contradiction_is_refused(self):
        command = JobCreate(
            description="x", job_mode="agent", config_override={"job_mode": "worker"}
        )
        with pytest.raises(HTTPException) as exc:
            admit_job_mode(command)
        assert exc.value.status_code == 400

    def test_the_same_value_twice_is_accepted(self):
        command = JobCreate(
            description="x", job_mode="agent", config_override={"job_mode": "agent"}
        )
        admit_job_mode(command)
        assert command.config_override == {"job_mode": "agent"}

    @pytest.mark.parametrize(
        "fields",
        [
            {"job_mode": "agent"},
            {"config_override": {"job_mode": "agent"}},
        ],
    )
    def test_agent_mode_with_a_live_session_is_refused(self, fields):
        command = JobCreate(description="x", live_session=True, **fields)
        with pytest.raises(HTTPException) as exc:
            admit_job_mode(command)
        assert exc.value.status_code == 422
        assert "agent mode" in exc.value.detail

    def test_worker_mode_with_a_live_session_is_accepted(self):
        command = JobCreate(description="x", live_session=True, job_mode="worker")
        admit_job_mode(command)


def _dependencies(**overrides) -> JobAdmissionDependencies:
    fields = dict(
        validate_tool_overrides=lambda override: override,
        enforce_readiness=AsyncMock(),
        scope=lambda: None,
        config=lambda: None,
        officer=lambda: None,
        workspace=lambda: None,
        datasources=lambda: None,
        delivery=lambda: None,
        creation=lambda: None,
        redact_result=lambda result: result,
    )
    fields.update(overrides)
    return JobAdmissionDependencies(**fields)


class _Stop(Exception):
    pass


@pytest.mark.asyncio
class TestAdmitJob:
    async def test_the_field_reaches_the_config_stage_in_the_override(
        self, monkeypatch
    ):
        seen: dict = {}

        async def scope(**_kwargs):
            return SimpleNamespace()

        async def config(*, command, **_kwargs):
            seen["override"] = copy.deepcopy(command.config_override)
            raise _Stop

        monkeypatch.setattr(job_admission, "prepare_job_admission_scope", scope)
        monkeypatch.setattr(job_admission, "prepare_job_admission_config", config)
        command = JobCreate(
            description="x", job_mode="agent", config_override={"autonomy": "full"}
        )
        with pytest.raises(HTTPException) as exc:
            await admit_job(
                command=command,
                actor=SimpleNamespace(),
                origin="public_rest",
                dependencies=_dependencies(),
            )
        # admit_job turns unexpected errors into a 500 after the config stage.
        assert exc.value.status_code == 500
        assert seen["override"] == {"autonomy": "full", "job_mode": "agent"}

    async def test_a_refusal_comes_before_readiness(self):
        dependencies = _dependencies()
        command = JobCreate(description="x", config_override={"job_mode": "fast"})
        with pytest.raises(HTTPException) as exc:
            await admit_job(
                command=command,
                actor=SimpleNamespace(),
                origin="public_rest",
                dependencies=dependencies,
            )
        assert exc.value.status_code == 400
        dependencies.enforce_readiness.assert_not_awaited()

    async def test_agent_mode_with_a_live_session_is_refused_first(self):
        dependencies = _dependencies()
        command = JobCreate(description="x", job_mode="agent", live_session=True)
        with pytest.raises(HTTPException) as exc:
            await admit_job(
                command=command,
                actor=SimpleNamespace(),
                origin="public_rest",
                dependencies=dependencies,
            )
        assert exc.value.status_code == 422
        assert "agent mode" in exc.value.detail
        dependencies.enforce_readiness.assert_not_awaited()


@pytest.mark.asyncio
class TestLiveVoiceSeesTheEffectiveMode:
    """A mode from an expert or project default reaches the resolved blob."""

    def _receptionist_row(self, **config):
        from pathlib import Path

        from shared.manifests import parse_documents
        from shared.runtime.core.srw_manifest_config import srw_private_config

        path = (
            Path(__file__).resolve().parents[1]
            / "config/experts/receptionist/config.yaml"
        )
        private = srw_private_config(parse_documents(path.read_text())[0])
        return {
            "name": "receptionist",
            "expert_type": "worker",
            "harness_adapter": "srw/v1",
            "harness_config_name": private["config_name"],
            "harness_asset_name": private.get("asset_name", "receptionist"),
            "harness_config_layers": private.get("layers", []),
            "config": {**(private.get("config") or {}), **config},
            "prompts": {},
        }

    async def test_a_worker_job_gets_its_voice(self):
        from orchestrator.services.live.admission import resolve_live_voice

        voice = await resolve_live_voice(
            MagicMock(),
            config_name="worker_base",
            expert_id=None,
            expert_row=self._receptionist_row(),
            config_override=None,
        )
        assert voice is not None

    async def test_an_expert_default_of_agent_mode_is_refused(self):
        from orchestrator.services.live.admission import resolve_live_voice
        from orchestrator.services.live.instructions import LiveVoiceError

        with pytest.raises(LiveVoiceError, match="agent mode"):
            await resolve_live_voice(
                MagicMock(),
                config_name="worker_base",
                expert_id=None,
                expert_row=self._receptionist_row(job_mode="agent"),
                config_override=None,
            )

    async def test_a_project_default_in_the_override_is_refused(self):
        from orchestrator.services.live.admission import resolve_live_voice
        from orchestrator.services.live.instructions import LiveVoiceError

        with pytest.raises(LiveVoiceError, match="agent mode"):
            await resolve_live_voice(
                MagicMock(),
                config_name="worker_base",
                expert_id=None,
                expert_row=self._receptionist_row(),
                config_override={"job_mode": "agent"},
            )


class TestSubJobsNeverInherit:
    def test_the_critic_override_states_worker(self):
        from orchestrator.services.verification_workflow import (
            critic_config_override,
        )

        assert critic_config_override(None)["job_mode"] == "worker"
        assert critic_config_override({"model": "m"})["job_mode"] == "worker"

    def test_a_project_default_cannot_put_the_critic_into_agent_mode(self):
        from orchestrator.services.config_resolver import resolve_config
        from orchestrator.services.verification_workflow import (
            critic_config_override,
        )

        blob = resolve_config(
            base_config_name="critic",
            expert_type="worker",
            project_overrides={"job_mode": "agent"},
            request_override=critic_config_override(None),
        )
        agent = blob["agent"]
        assert agent["job_mode"] == "worker"
        assert "next_phase_todos" in agent["tools"]["core"]
        assert "agent-mode" not in {e.get("skill") for e in agent["instruction_files"]}

    @pytest.mark.asyncio
    async def test_the_scholar_override_states_worker(self, monkeypatch):
        from orchestrator.services.subjob_completion import (
            ScholarCompletionDependencies,
            spawn_scholar_subjob,
        )

        store = MagicMock()
        store.get_user = AsyncMock(return_value={"id": "user-1"})
        store.update_job_status = AsyncMock()
        store.merge_job_context = AsyncMock()
        store.create_job = AsyncMock(side_effect=_Stop)
        dependencies = ScholarCompletionDependencies(
            store=store,
            forge=MagicMock(),
            trigger_dispatch=MagicMock(),
            resolve_workspace_backend=lambda _job: "sandbox",
            is_lite_config_override=lambda _override: False,
            should_provision_parent_container=lambda _override: False,
            revalidate_datasource_selection=AsyncMock(return_value=([], {})),
            datasource_selection_provenance=AsyncMock(return_value={}),
            prepare_primary_repository_authority=AsyncMock(return_value=None),
            completion_resume_guard_kwargs=lambda: {},
            maybe_wake_session=AsyncMock(),
            kick_session_wake_drain=MagicMock(),
            notify_review_returned=AsyncMock(),
        )
        monkeypatch.setattr(
            "orchestrator.services.completion.resolve_scholar_config_from_disk",
            lambda *_args, **_kwargs: {"enabled": True, "scholar_config": "scholar"},
        )
        monkeypatch.setattr(
            "orchestrator.services.completion.format_scholar_instructions",
            lambda **_kwargs: "instructions",
        )
        with pytest.raises(_Stop):
            await spawn_scholar_subjob(
                {"id": "parent-1", "user_id": "user-1", "description": "d"},
                "worker_base",
                {"job_mode": "agent", "llm": {"model": "m"}},
                {},
                dependencies=dependencies,
            )
        override = store.create_job.await_args.kwargs["config_override"]
        assert override["job_mode"] == "worker"
        assert override["llm"] == {"model": "m"}


class TestReadBack:
    """``job_mode`` on the job detail: the frozen configuration first, then
    the requested override, else ``worker``."""

    @pytest.mark.parametrize(
        "row,expected",
        [
            ({}, "worker"),
            ({"config_override": None, "resolved_config": None}, "worker"),
            ({"config_override": {"job_mode": "agent"}}, "agent"),
            ({"config_override": '{"job_mode": "agent"}'}, "agent"),
            ({"resolved_config": {"agent": {"job_mode": "agent"}}}, "agent"),
            ({"resolved_config": '{"agent": {"job_mode": "agent"}}'}, "agent"),
            (
                {
                    "resolved_config": {"agent": {"job_mode": "worker"}},
                    "config_override": {"job_mode": "agent"},
                },
                "worker",
            ),
            (
                {
                    "resolved_config": {"agent": {"autonomy": "full"}},
                    "config_override": {"job_mode": "agent"},
                },
                "agent",
            ),
            ({"config_override": {"job_mode": "bogus"}}, "worker"),
            ({"config_override": "not json"}, "worker"),
        ],
    )
    def test_effective_job_mode(self, row, expected):
        from orchestrator.services.job_projection import effective_job_mode

        assert effective_job_mode(row) == expected

    @pytest.mark.asyncio
    async def test_the_detail_carries_the_frozen_mode(self):
        from orchestrator.services.job_reads import JobReadDependencies, read_job

        def redact(job):
            job = dict(job)
            job.pop("resolved_config", None)
            return job

        dependencies = JobReadDependencies(
            store=MagicMock(),
            audit_reader=SimpleNamespace(is_available=False),
            redact_job=redact,
            with_cloud_review_mode=lambda job: job,
            status_filter_values=(),
        )
        row = {
            "id": "job-1",
            "config_override": None,
            "resolved_config": {"agent": {"job_mode": "agent"}},
        }
        result = await read_job(
            job_id="job-1", authorized_job=row, dependencies=dependencies
        )
        assert result["job_mode"] == "agent"
