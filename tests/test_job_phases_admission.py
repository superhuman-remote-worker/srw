"""Whether a job plans in phases, at admission: the field, its checks, the stamp.

``phases`` on ``JobCreate`` is folded into ``config_override`` before the
config stage reads it, so the resolver freezes it with the rest of the job's
configuration. Values that are neither true nor false and contradictions are
refused, as is ``phases: false`` on a live-session job. Once the expert is
final, admission writes the job's effective value on its override
(``stamp_job_phases``). Sub-jobs (the verification critic, the scholar pass)
always state ``phases: true`` and never inherit a parent's or a default's
value.
"""

from __future__ import annotations

import copy
import logging
from pathlib import Path
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
    admit_job_phases,
    resolved_phases_override,
    stamp_job_phases,
)
from orchestrator.services.job_admission_officer import JobAdmissionOfficer

EXPERTS = Path(__file__).resolve().parents[1] / "config" / "experts"


def _bundled_row(name: str, **config) -> dict:
    """The resolver row of a bundled expert, as the catalogue stores it."""
    from shared.manifests import parse_documents
    from shared.runtime.core.srw_manifest_config import srw_private_config

    path = EXPERTS / name / "config.yaml"
    private = srw_private_config(parse_documents(path.read_text())[0])
    return {
        "name": name,
        "expert_type": "worker",
        "harness_adapter": "srw/v1",
        "harness_config_name": private["config_name"],
        "harness_asset_name": private.get("asset_name", name),
        "harness_config_layers": private.get("layers", []),
        "config": {**(private.get("config") or {}), **config},
        "prompts": {},
    }


class TestTheRequestField:
    def test_it_is_public_and_optional(self):
        assert "phases" in PUBLIC_JOB_CREATE_FIELDS
        assert JobCreate(description="x").phases is None
        assert JobCreate(description="x", phases=False).phases is False
        assert JobCreate(description="x", phases=True).phases is True

    @pytest.mark.parametrize(
        "value",
        ["light", "agent", 2, ["on"], 1, 0, "1", "0", "yes", "on", "true", "false"],
    )
    def test_a_value_that_is_no_boolean_is_refused_by_the_schema(self, value):
        # Strict: only JSON true/false (or null) pass the REST field; the
        # on/off words are for an override written by hand.
        with pytest.raises(ValidationError):
            JobCreate(description="x", phases=value)

    @pytest.mark.parametrize(
        "body,expected",
        [
            ('"phases": true', True),
            ('"phases": false', False),
            ('"phases": null', None),
        ],
    )
    def test_json_booleans_and_null_pass(self, body, expected):
        command = JobCreate.model_validate_json(f'{{"description": "x", {body}}}')
        assert command.phases is expected

    @pytest.mark.parametrize("body", ['"phases": "true"', '"phases": 1'])
    def test_json_text_and_numbers_are_refused(self, body):
        with pytest.raises(ValidationError):
            JobCreate.model_validate_json(f'{{"description": "x", {body}}}')

    def test_the_public_schema_documents_a_boolean(self):
        from orchestrator.schemas.job_create import public_job_create_schema

        field = public_job_create_schema()["properties"]["phases"]
        types = {branch.get("type") for branch in field.get("anyOf", [field])}
        assert "boolean" in types
        assert field.get("default") is None
        assert "job_complete" in field["description"]
        assert "live_session" in field["description"]


class TestAdmitJobPhases:
    def test_the_field_is_folded_into_the_override(self):
        command = JobCreate(
            description="x",
            phases=False,
            config_override={"autonomy": "full", "llm": {"model": "m"}},
        )
        admit_job_phases(command)
        assert command.config_override == {
            "autonomy": "full",
            "llm": {"model": "m"},
            "phases": False,
        }

    def test_the_field_alone_creates_the_override(self):
        command = JobCreate(description="x", phases=True)
        admit_job_phases(command)
        assert command.config_override == {"phases": True}

    def test_without_the_field_nothing_changes(self):
        command = JobCreate(description="x", config_override={"autonomy": "full"})
        admit_job_phases(command)
        assert command.config_override == {"autonomy": "full"}
        command = JobCreate(description="x")
        admit_job_phases(command)
        assert command.config_override is None

    @pytest.mark.parametrize(
        "value,expected",
        [
            (True, True),
            (False, False),
            ("on", True),
            ("off", False),
            ("TRUE", True),
            (" No ", False),
        ],
    )
    def test_a_valid_override_value_is_normalized(self, value, expected):
        command = JobCreate(description="x", config_override={"phases": value})
        admit_job_phases(command)
        assert command.config_override == {"phases": expected}

    @pytest.mark.parametrize("value", [None, ""])
    def test_an_empty_override_value_is_dropped(self, value):
        # Null means "what the configuration says"; left in the override it
        # would delete the expert's own value at the merge.
        command = JobCreate(
            description="x", config_override={"autonomy": "full", "phases": value}
        )
        admit_job_phases(command)
        assert command.config_override == {"autonomy": "full"}

    @pytest.mark.parametrize("value", [None, ""])
    def test_the_field_replaces_an_empty_override_value(self, value):
        command = JobCreate(
            description="x", phases=False, config_override={"phases": value}
        )
        admit_job_phases(command)
        assert command.config_override == {"phases": False}

    @pytest.mark.parametrize("value", ["light", "agent", 1, 0, ["on"], {"on": 1}])
    def test_an_unknown_override_value_is_refused(self, value):
        command = JobCreate(description="x", config_override={"phases": value})
        with pytest.raises(HTTPException) as exc:
            admit_job_phases(command)
        assert exc.value.status_code == 400
        assert "config_override.phases must be true or false" in exc.value.detail

    @pytest.mark.parametrize(
        "field,stated", [(False, True), (True, "off"), (False, "on")]
    )
    def test_a_contradiction_is_refused(self, field, stated):
        command = JobCreate(
            description="x", phases=field, config_override={"phases": stated}
        )
        with pytest.raises(HTTPException) as exc:
            admit_job_phases(command)
        assert exc.value.status_code == 400
        assert "state it once" in exc.value.detail

    def test_the_same_value_twice_is_accepted(self):
        command = JobCreate(
            description="x", phases=False, config_override={"phases": "off"}
        )
        admit_job_phases(command)
        assert command.config_override == {"phases": False}

    @pytest.mark.parametrize(
        "fields",
        [
            {"phases": False},
            {"config_override": {"phases": False}},
            {"config_override": {"phases": "off"}},
        ],
    )
    def test_no_phases_with_a_live_session_is_refused(self, fields):
        command = JobCreate(description="x", live_session=True, **fields)
        with pytest.raises(HTTPException) as exc:
            admit_job_phases(command)
        assert exc.value.status_code == 422
        assert "always plans in phases" in exc.value.detail

    @pytest.mark.parametrize("fields", [{"phases": True}, {}])
    def test_phases_or_nothing_with_a_live_session_is_accepted(self, fields):
        command = JobCreate(description="x", live_session=True, **fields)
        admit_job_phases(command)


def _refuse_resolve(monkeypatch) -> None:
    def refuse(**_kwargs):
        raise AssertionError("the expert was resolved")

    monkeypatch.setattr("orchestrator.services.config_resolver.resolve_config", refuse)


@pytest.mark.asyncio
class TestStampJobPhases:
    """``stamp_job_phases`` writes the job's effective value on its override."""

    @pytest.mark.parametrize(
        "stated,expected", [(True, True), (False, False), ("on", True), ("off", False)]
    )
    async def test_the_jobs_own_value_is_kept(self, monkeypatch, stated, expected):
        _refuse_resolve(monkeypatch)
        override = {"autonomy": "full", "phases": stated}
        before = copy.deepcopy(override)
        stamped = await stamp_job_phases(
            command=JobCreate(description="x"),
            config_name="developer",
            expert_row=None,
            config_override=override,
            harness_image=None,
        )
        assert stamped == {"autonomy": "full", "phases": expected}
        assert override == before

    @pytest.mark.parametrize("override", [None, {}, {"autonomy": "full"}])
    async def test_a_live_session_job_always_plans_in_phases(
        self, monkeypatch, override
    ):
        _refuse_resolve(monkeypatch)
        stamped = await stamp_job_phases(
            command=JobCreate(description="x", live_session=True),
            config_name="general-worker",
            expert_row=None,
            config_override=override,
            harness_image=None,
        )
        assert stamped == {**(override or {}), "phases": True}

    @pytest.mark.parametrize(
        "name,expected", [("developer", True), ("general-worker", False)]
    )
    async def test_a_bundled_selector_gives_its_experts_value(self, name, expected):
        stamped = await stamp_job_phases(
            command=JobCreate(description="x"),
            config_name=name,
            expert_row=None,
            config_override={"autonomy": "full"},
            harness_image=None,
        )
        assert stamped == {"autonomy": "full", "phases": expected}

    @pytest.mark.parametrize(
        "name,expected", [("developer", True), ("general-worker", False)]
    )
    async def test_a_stored_expert_row_gives_its_value(self, name, expected):
        row = _bundled_row(name)
        before = copy.deepcopy(row)
        stamped = await stamp_job_phases(
            command=JobCreate(description="x"),
            config_name="worker_base",
            expert_row=row,
            config_override=None,
            harness_image=None,
        )
        assert stamped == {"phases": expected}
        assert row == before

    async def test_an_expert_edited_to_phases_is_read(self):
        stamped = await stamp_job_phases(
            command=JobCreate(description="x"),
            config_name="worker_base",
            expert_row=_bundled_row("general-worker", phases=True),
            config_override=None,
            harness_image=None,
        )
        assert stamped == {"phases": True}

    async def test_without_an_expert_the_framework_default_applies(self):
        stamped = await stamp_job_phases(
            command=JobCreate(description="x"),
            config_name=None,
            expert_row=None,
            config_override=None,
            harness_image=None,
        )
        assert stamped == {"phases": False}

    async def test_a_custom_harness_image_gets_no_value(self, monkeypatch):
        _refuse_resolve(monkeypatch)
        for override in (None, {"autonomy": "full"}):
            stamped = await stamp_job_phases(
                command=JobCreate(description="x"),
                config_name="developer",
                expert_row=None,
                config_override=override,
                harness_image="example/custom@sha256:" + "c" * 64,
            )
            assert stamped is override

    async def test_a_resolve_error_leaves_the_override_unchanged(
        self, monkeypatch, caplog
    ):
        def broken(**_kwargs):
            raise RuntimeError("the roster does not resolve")

        monkeypatch.setattr(
            "orchestrator.services.config_resolver.resolve_config", broken
        )
        override = {"autonomy": "full"}
        with caplog.at_level(logging.WARNING, logger=job_admission.__name__):
            stamped = await stamp_job_phases(
                command=JobCreate(description="x"),
                config_name="developer",
                expert_row=None,
                config_override=override,
                harness_image=None,
            )
        assert stamped is override
        assert override == {"autonomy": "full"}
        assert "phases setting could not be resolved" in caplog.text
        assert "the roster does not resolve" in caplog.text

    @pytest.mark.parametrize(
        "config_name,expected", [("developer", True), ("general-worker", False)]
    )
    async def test_the_stamp_and_the_shared_helper_agree(self, config_name, expected):
        # resolved_phases_override is what manifest admission calls as well.
        for live in (False, True):
            stamped = await stamp_job_phases(
                command=JobCreate(description="x", live_session=live),
                config_name=config_name,
                expert_row=None,
                config_override={"autonomy": "full"},
                harness_image=None,
            )
            shared = await resolved_phases_override(
                live_session=live,
                config_name=config_name,
                expert_row=None,
                config_override={"autonomy": "full"},
            )
            assert (
                stamped
                == shared
                == {
                    "autonomy": "full",
                    "phases": True if live else expected,
                }
            )

    async def test_an_unknown_expert_leaves_the_override_unchanged(self, caplog):
        with caplog.at_level(logging.WARNING, logger=job_admission.__name__):
            stamped = await stamp_job_phases(
                command=JobCreate(description="x"),
                config_name="no-such-expert-anywhere",
                expert_row=None,
                config_override=None,
                harness_image=None,
            )
        assert stamped is None
        assert "phases setting could not be resolved" in caplog.text


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
            description="x", phases=False, config_override={"autonomy": "full"}
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
        assert seen["override"] == {"autonomy": "full", "phases": False}

    @pytest.mark.parametrize(
        "config_name,requested,expected",
        [
            ("developer", None, True),
            ("general-worker", None, False),
            ("general-worker", True, True),
        ],
    )
    async def test_the_stamped_value_reaches_the_later_stages(
        self, monkeypatch, config_name, requested, expected
    ):
        seen: dict = {}

        async def scope(**_kwargs):
            return SimpleNamespace(user_id="user-1", principal={}, origin_bound=False)

        async def config(*, command, **_kwargs):
            return SimpleNamespace(
                expert_source="caller",
                pending_harness_expert=None,
                config_name=config_name,
                expert_id=None,
                expert_row=None,
                harness_image=None,
                workspace_selection=None,
                project_id=None,
                root_creation=True,
            )

        async def officer(*, command, **_kwargs):
            return JobAdmissionOfficer(
                context={},
                config_override=dict(command.config_override or {}),
                preparation=None,
                ticket_ready_at=None,
            )

        async def workspace(*, config_override, **_kwargs):
            seen["override"] = copy.deepcopy(config_override)
            raise _Stop

        monkeypatch.setattr(job_admission, "prepare_job_admission_scope", scope)
        monkeypatch.setattr(job_admission, "prepare_job_admission_config", config)
        monkeypatch.setattr(job_admission, "prepare_job_admission_officer", officer)
        monkeypatch.setattr(job_admission, "prepare_job_admission_workspace", workspace)
        command = JobCreate(
            description="x", phases=requested, config_override={"autonomy": "full"}
        )
        with pytest.raises(HTTPException) as exc:
            await admit_job(
                command=command,
                actor=SimpleNamespace(),
                origin="public_rest",
                dependencies=_dependencies(),
            )
        assert exc.value.status_code == 500
        assert seen["override"] == {"autonomy": "full", "phases": expected}

    async def test_a_refusal_comes_before_readiness(self):
        dependencies = _dependencies()
        command = JobCreate(description="x", config_override={"phases": "fast"})
        with pytest.raises(HTTPException) as exc:
            await admit_job(
                command=command,
                actor=SimpleNamespace(),
                origin="public_rest",
                dependencies=dependencies,
            )
        assert exc.value.status_code == 400
        dependencies.enforce_readiness.assert_not_awaited()

    async def test_no_phases_with_a_live_session_is_refused_first(self):
        dependencies = _dependencies()
        command = JobCreate(description="x", phases=False, live_session=True)
        with pytest.raises(HTTPException) as exc:
            await admit_job(
                command=command,
                actor=SimpleNamespace(),
                origin="public_rest",
                dependencies=dependencies,
            )
        assert exc.value.status_code == 422
        assert "always plans in phases" in exc.value.detail
        dependencies.enforce_readiness.assert_not_awaited()


@pytest.mark.asyncio
class TestLiveVoiceSeesTheEffectiveValue:
    """A value from an expert or project default reaches the resolved blob."""

    async def test_a_phased_job_gets_its_voice(self):
        from orchestrator.services.live.admission import resolve_live_voice

        voice = await resolve_live_voice(
            MagicMock(),
            config_name="worker_base",
            expert_id=None,
            expert_row=_bundled_row("receptionist"),
            config_override=None,
        )
        assert voice is not None

    async def test_an_expert_without_phases_is_refused(self):
        from orchestrator.services.live.admission import resolve_live_voice
        from orchestrator.services.live.instructions import LiveVoiceError

        with pytest.raises(LiveVoiceError, match="without phases"):
            await resolve_live_voice(
                MagicMock(),
                config_name="worker_base",
                expert_id=None,
                expert_row=_bundled_row("receptionist", phases=False),
                config_override=None,
            )

    async def test_an_expert_that_does_not_state_it_is_refused(self):
        # The backstop: admission stamps phases: true on every live-session
        # job before it asks for the voice.
        from orchestrator.services.live.admission import resolve_live_voice
        from orchestrator.services.live.instructions import LiveVoiceError

        row = _bundled_row("receptionist")
        row["config"].pop("phases")
        with pytest.raises(LiveVoiceError, match="without phases"):
            await resolve_live_voice(
                MagicMock(),
                config_name="worker_base",
                expert_id=None,
                expert_row=row,
                config_override=None,
            )
        voice = await resolve_live_voice(
            MagicMock(),
            config_name="worker_base",
            expert_id=None,
            expert_row=row,
            config_override={"phases": True},
        )
        assert voice is not None

    async def test_a_project_default_in_the_override_is_refused(self):
        from orchestrator.services.live.admission import resolve_live_voice
        from orchestrator.services.live.instructions import LiveVoiceError

        with pytest.raises(LiveVoiceError, match="without phases"):
            await resolve_live_voice(
                MagicMock(),
                config_name="worker_base",
                expert_id=None,
                expert_row=_bundled_row("receptionist"),
                config_override={"phases": False},
            )


class TestSubJobsNeverInherit:
    def test_the_critic_override_states_phases(self):
        from orchestrator.services.verification_workflow import (
            critic_config_override,
        )

        assert critic_config_override(None)["phases"] is True
        assert critic_config_override({"model": "m"})["phases"] is True

    def test_a_project_default_cannot_take_the_critics_phases(self):
        from orchestrator.services.config_resolver import resolve_config
        from orchestrator.services.verification_workflow import (
            critic_config_override,
        )

        blob = resolve_config(
            base_config_name="critic",
            expert_type="worker",
            project_overrides={"phases": False},
            request_override=critic_config_override(None),
        )
        agent = blob["agent"]
        assert agent["phases"] is True
        assert "next_phase_todos" in agent["tools"]["core"]
        assert "direct-task" not in {e.get("skill") for e in agent["instruction_files"]}

    @pytest.mark.asyncio
    async def test_the_scholar_override_states_phases(self, monkeypatch):
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
                {"phases": False, "llm": {"model": "m"}},
                {},
                dependencies=dependencies,
            )
        override = store.create_job.await_args.kwargs["config_override"]
        assert override["phases"] is True
        assert override["llm"] == {"model": "m"}


class TestReadBack:
    """``phases`` on the job detail: the frozen configuration first, then the
    value admission wrote on the override, else ``None`` (unknown)."""

    @pytest.mark.parametrize(
        "row,expected",
        [
            ({}, None),
            ({"config_override": None, "resolved_config": None}, None),
            ({"config_override": {"phases": False}}, False),
            ({"config_override": {"phases": True}}, True),
            ({"config_override": '{"phases": false}'}, False),
            ({"config_override": {"phases": "on"}}, True),
            ({"resolved_config": {"agent": {"phases": False}}}, False),
            ({"resolved_config": '{"agent": {"phases": true}}'}, True),
            (
                {
                    "resolved_config": {"agent": {"phases": True}},
                    "config_override": {"phases": False},
                },
                True,
            ),
            (
                {
                    "resolved_config": {"agent": {"autonomy": "full"}},
                    "config_override": {"phases": False},
                },
                False,
            ),
            ({"config_override": {"phases": "bogus"}}, None),
            ({"config_override": "not json"}, None),
        ],
    )
    def test_effective_phases(self, row, expected):
        from orchestrator.services.job_projection import effective_phases

        assert effective_phases(row) is expected

    @pytest.mark.asyncio
    async def test_the_detail_carries_the_frozen_value(self):
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
            "config_override": {"phases": True},
            "resolved_config": {"agent": {"phases": False}},
        }
        result = await read_job(
            job_id="job-1", authorized_job=row, dependencies=dependencies
        )
        assert result["phases"] is False


class TestStampReadsAStoredExpertById:
    """A job that names a stored expert carries only its id through admission
    (the row is read at dispatch), so the stamp reads the expert itself.
    Without that the job took the base's value: a stored expert with
    ``phases: true`` ran without phases (seen live on k3d, 2026-11-17)."""

    @staticmethod
    def _command(**fields):
        from orchestrator.schemas.job_create import JobCreate

        return JobCreate(description="task", **fields)

    @staticmethod
    def _row(phases):
        config = {} if phases is None else {"phases": phases}
        return {
            "id": "e-1",
            "name": "custom",
            "expert_type": "worker",
            "config": config,
            "prompts": {},
        }

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "stored, expected", [(True, True), (False, False), (None, False)]
    )
    async def test_the_stored_experts_value_is_taken(self, stored, expected):
        from orchestrator.services import job_admission

        seen = []

        async def load_expert(expert_id):
            seen.append(expert_id)
            return self._row(stored)

        override = await job_admission.stamp_job_phases(
            command=self._command(),
            config_name="worker_base",
            expert_row=None,
            config_override={"autonomy": "full"},
            harness_image=None,
            expert_id="e-1",
            load_expert=load_expert,
        )

        assert seen == ["e-1"]
        assert override == {"autonomy": "full", "phases": expected}

    @pytest.mark.asyncio
    @pytest.mark.parametrize("own", [True, False])
    async def test_the_jobs_own_value_needs_no_read(self, own):
        from orchestrator.services import job_admission

        async def load_expert(expert_id):  # pragma: no cover - must not run
            raise AssertionError(
                "the expert was read although the job states its value"
            )

        override = await job_admission.stamp_job_phases(
            command=self._command(),
            config_name="worker_base",
            expert_row=None,
            config_override={"phases": own},
            harness_image=None,
            expert_id="e-1",
            load_expert=load_expert,
        )

        assert override == {"phases": own}

    @pytest.mark.asyncio
    @pytest.mark.parametrize("failure", ["raises", "missing"])
    async def test_an_expert_that_cannot_be_read_leaves_the_override_unchanged(
        self, failure, caplog
    ):
        from orchestrator.services import job_admission

        async def load_expert(expert_id):
            if failure == "raises":
                raise RuntimeError("database is away")
            return None

        given = {"autonomy": "full"}
        with caplog.at_level("WARNING"):
            override = await job_admission.stamp_job_phases(
                command=self._command(),
                config_name="worker_base",
                expert_row=None,
                config_override=given,
                harness_image=None,
                expert_id="e-1",
                load_expert=load_expert,
            )

        assert override == given
        assert "phases" in caplog.text
