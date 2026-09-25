"""P1-A non-destructive steer: the supervisor guidance lane.

knowledge-base/knowledge/issues/officer_blind_reads_and_worker_bureaucracy.md §4 P1-A / §7 annex B.

Before this change BOTH steering verbs were broken: an urgent steer was a
hidden resume-with-feedback (force-compaction + silent todo wipe + false
"frozen for human review" banner + template-mandated re-plan — the officer's
steer destroyed the very work it demanded), and a queued steer was a dead
letter box (files nothing read, re-materialized at every boundary because the
clearing contract was never implemented).

Covered here:
  - orchestrator routing: urgent → ``context.pending_guidance`` append, NO
    resume/pause; resume fallback only when there is no live run
  - heartbeat response carries ``pending_guidance`` from the same row read
  - dual_app inbox stores/prunes entries; ack fires the client call
  - the execute node renders exactly ONE [SUPERVISOR GUIDANCE] block with all
    pending entries and acks them after the LLM turn
  - queued-reply drain injects content into visible context, acks the drained
    threads, and does not re-materialize a cleared thread
  - the [FEEDBACK_RESUME] banner states the passed reason (honest fallback
    otherwise — never the blanket "previously frozen for human review")
  - restore_from_feedback archives in-flight todos instead of discarding them
"""

import asyncio
import contextlib
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# R1.B06: the heartbeat moved to services/agent_registration.
from orchestrator.services import agent_registration  # noqa: E402
from orchestrator.services import inbound_reply, job_guidance  # noqa: E402
from fastapi import HTTPException
from langchain_core.messages import AIMessage, HumanMessage

import agent.api.dual_app as dual_app  # noqa: E402
from agent.core.guidance_injection import format_supervisor_guidance  # noqa: E402
from agent.core.workspace import WorkspaceManager  # noqa: E402
from agent.managers import TodoManager  # noqa: E402
from tests._fs_backend import FilesystemTestBackend  # noqa: E402
from orchestrator.application import sessions as sessions_composition
from orchestrator.application import workflows as workflows_composition
from orchestrator.schemas import agent_runtime as agent_runtime_module
from orchestrator.schemas import messaging as messaging_module
from orchestrator.security import access as access_module
from orchestrator.services import job_controls as job_controls_module


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def temp_workspace():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def workspace_manager(temp_workspace):
    ws = WorkspaceManager(
        job_id="test-job-123",
        base_path=temp_workspace,
        backend=FilesystemTestBackend(temp_workspace),
    )
    ws.initialize()
    return ws


@pytest.fixture
def todo_manager(workspace_manager):
    return TodoManager(workspace_manager)


@pytest.fixture
def mock_config():
    """config.extra must be a real dict (MagicMock -> yaml.safe_load loop)."""
    config = MagicMock()
    config.agent_id = "test-agent"
    config.llm.model = "test-model"
    config.extra = {}
    config._deployment_dir = None
    return config


JOB_ID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"


def _job(status="processing", freeze_data=None):
    return {
        "id": JOB_ID,
        "status": status,
        "freeze_data": freeze_data,
        "user_id": None,
        "description": "job under test",
        "phase_number": 2,
    }


def _routing_db(job):
    db = AsyncMock()
    db.get_job.return_value = job
    db.get_message_sequence.return_value = 7
    db.get_user_settings.return_value = {}
    db.append_pending_guidance.return_value = True
    db.append_queued_reply.return_value = True
    return db


def _route_inbound_reply(om, *args, **kwargs):
    """Drive the extracted reply funnel with the application's collaborators.

    ``main._inbound_reply_dependencies()`` binds ``main.postgres_db``,
    ``main.notification_service``, the completion-control boundary, and
    ``control_seams.internal_resume_job``
    at call time, so building it inside the patch scope keeps every patch below
    steering the code under test.
    """
    return inbound_reply.route_inbound_reply(
        *args,
        **kwargs,
        dependencies=workflows_composition.inbound_reply_dependencies(
            om.app.state.resources
        ),
    )


def _ack_job_guidance(om, request, job_id, body):
    """Same, for the guidance ack behind ``POST /api/jobs/{id}/guidance/ack``."""
    return job_guidance.ack_job_guidance(
        request,
        job_id,
        body,
        dependencies=workflows_composition.job_guidance_dependencies(
            om.app.state.resources
        ),
    )


# =============================================================================
# Orchestrator routing: urgent ≠ resume anymore
# =============================================================================


class TestUrgentReplyRoutesToGuidance:
    @pytest.mark.asyncio
    async def test_completion_guard_blocks_before_reply_log_or_context_mutation(self):
        import orchestrator.main as om

        db = _routing_db(_job(status="processing"))
        guard = AsyncMock(
            side_effect=HTTPException(status_code=409, detail="completion finalizing")
        )
        with (
            patch.object(om.app.state.resources, "postgres_db", db),
            patch.object(
                om.app.state.resources.completion_control_boundary, "guard", guard
            ),
            pytest.raises(HTTPException) as exc,
        ):
            await _route_inbound_reply(
                om, JOB_ID, "officer", "do not race completion", urgent=True
            )

        assert exc.value.status_code == 409
        guard.assert_awaited_once_with(JOB_ID, source="inbound_reply")
        db.get_message_sequence.assert_not_awaited()
        db.log_message.assert_not_awaited()
        db.append_pending_guidance.assert_not_awaited()
        db.append_queued_reply.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_immediate_resume_cas_loss_is_conflict_not_success(self):
        import orchestrator.main as om

        job = _job(status="paused")
        db = _routing_db(job)
        resume = AsyncMock(return_value=False)
        guard = AsyncMock()
        with (
            patch.object(om.app.state.resources, "postgres_db", db),
            patch.object(
                om.app.state.resources.completion_control_boundary, "guard", guard
            ),
            patch.object(
                job_controls_module.JobControlOperations,
                "internal_resume_job",
                resume,
            ),
            pytest.raises(HTTPException) as exc,
        ):
            await _route_inbound_reply(
                om, JOB_ID, "officer", "wake only if fenced", urgent=True
            )

        assert exc.value.status_code == 409
        assert guard.await_count == 2
        resume.assert_awaited_once()
        db.log_message.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_flag_on_queued_reply_losing_command_cas_is_clean_conflict(self):
        import orchestrator.main as om

        db = _routing_db(_job(status="processing"))
        db.append_queued_reply.return_value = False
        guard = AsyncMock()
        with (
            patch.object(
                om.app.state.resources.settings, "completion_commands_enabled", True
            ),
            patch.object(om.app.state.resources, "postgres_db", db),
            patch.object(
                om.app.state.resources.completion_control_boundary, "guard", guard
            ),
            pytest.raises(HTTPException) as exc,
        ):
            await _route_inbound_reply(
                om, JOB_ID, "officer", "queue only if I win", urgent=False
            )

        assert exc.value.status_code == 409
        db.append_queued_reply.assert_awaited_once()
        assert db.append_queued_reply.await_args.kwargs == {
            "completion_commands_enabled": True
        }
        db.log_message.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_urgent_on_processing_job_appends_guidance_no_resume(self):
        """The P1-A headline: urgent steer on a live run appends to
        context.pending_guidance and triggers NO resume/pause."""
        import orchestrator.main as om

        db = _routing_db(_job(status="processing"))
        resume = AsyncMock()
        with (
            patch.object(om.app.state.resources, "postgres_db", db),
            patch.object(
                job_controls_module.JobControlOperations,
                "internal_resume_job",
                resume,
            ),
        ):
            strategy, sequence = await _route_inbound_reply(
                om, JOB_ID, "officer", "stop retrying X, read file Z", urgent=True
            )

        assert strategy == "guidance_next_turn"
        assert sequence == 7
        resume.assert_not_awaited()

        db.append_pending_guidance.assert_awaited_once()
        appended_job_id, entry = db.append_pending_guidance.await_args.args
        assert appended_job_id == JOB_ID
        assert entry["text"] == "stop retrying X, read file Z"
        assert entry["source"] == "officer"
        assert entry["id"]
        assert entry["created_at"]

    @pytest.mark.asyncio
    async def test_urgent_on_paused_job_falls_back_to_resume_with_reason(self):
        """No live run to steer -> the old resume semantics, honestly labelled."""
        import orchestrator.main as om

        db = _routing_db(_job(status="paused"))
        resume = AsyncMock()
        with (
            patch.object(om.app.state.resources, "postgres_db", db),
            patch.object(
                job_controls_module.JobControlOperations,
                "internal_resume_job",
                resume,
            ),
        ):
            strategy, _ = await _route_inbound_reply(
                om, JOB_ID, "officer", "wake up and do Y", urgent=True
            )

        assert strategy == "immediate_interrupt"
        db.append_pending_guidance.assert_not_awaited()
        resume.assert_awaited_once()
        assert resume.await_args.kwargs["reason"] == inbound_reply.URGENT_RESUME_REASON

    @pytest.mark.asyncio
    async def test_urgent_on_operator_paused_job_reports_queued(self):
        """The hold keeps the job parked: the message waits for the resume."""
        import orchestrator.main as om

        held = {
            **_job(status="paused"),
            "context": {"_operator_pause_hold": {"version": 1, "hold_id": "h1"}},
        }
        db = _routing_db(held)
        resume = AsyncMock(return_value=True)
        with (
            patch.object(om.app.state.resources, "postgres_db", db),
            patch.object(
                job_controls_module.JobControlOperations,
                "internal_resume_job",
                resume,
            ),
        ):
            strategy, _ = await _route_inbound_reply(
                om, JOB_ID, "officer", "wake up and do Y", urgent=True
            )

        assert strategy == "queued_until_resume"
        resume.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_blocking_reply_still_resumes_with_honest_reason(self):
        """A reply the job froze waiting for keeps resuming — that is the
        correct verb there — and now carries the actual cause."""
        import orchestrator.main as om

        db = _routing_db(
            _job(status="waiting_for_reply", freeze_data={"thread_id": "t1"})
        )
        resume = AsyncMock()
        with (
            patch.object(om.app.state.resources, "postgres_db", db),
            patch.object(
                job_controls_module.JobControlOperations,
                "internal_resume_job",
                resume,
            ),
        ):
            strategy, _ = await _route_inbound_reply(om, JOB_ID, "t1", "the answer")

        assert strategy == "immediate_resume"
        resume.assert_awaited_once()
        assert "reply" in resume.await_args.kwargs["reason"]
        db.append_pending_guidance.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_immediate_interrupt_pref_uses_guidance_on_live_run(self):
        """The user-preference interrupt arm rides the same guidance lane."""
        import orchestrator.main as om

        job = _job(status="processing")
        job["user_id"] = "11111111-2222-3333-4444-555555555555"
        db = _routing_db(job)
        db.get_user_settings.return_value = {
            "communication": {"delivery": {"async_reply": "immediate_interrupt"}}
        }
        resume = AsyncMock()
        with (
            patch.object(om.app.state.resources, "postgres_db", db),
            patch.object(
                job_controls_module.JobControlOperations,
                "internal_resume_job",
                resume,
            ),
        ):
            strategy, _ = await _route_inbound_reply(
                om, JOB_ID, "officer", "adjust course", urgent=False
            )

        assert strategy == "guidance_next_turn"
        resume.assert_not_awaited()
        db.append_pending_guidance.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_non_urgent_reply_still_queues(self):
        """The queued lane is untouched at routing time."""
        import orchestrator.main as om

        db = _routing_db(_job(status="processing"))
        resume = AsyncMock()
        with (
            patch.object(om.app.state.resources, "postgres_db", db),
            patch.object(
                job_controls_module.JobControlOperations,
                "internal_resume_job",
                resume,
            ),
        ):
            strategy, _ = await _route_inbound_reply(
                om, JOB_ID, "officer", "for the next boundary", urgent=False
            )

        assert strategy == "next_strategic_phase"
        resume.assert_not_awaited()
        db.append_pending_guidance.assert_not_awaited()
        db.append_queued_reply.assert_awaited_once()
        queued = db.append_queued_reply.await_args.args[1]
        assert queued["id"]
        assert queued["thread_id"] == "officer"


class TestCheckpointCoupledAckEndpoint:
    @pytest.mark.asyncio
    async def test_forwards_exact_checkpoint_delivery_sets(self):
        import orchestrator.main as om

        db = AsyncMock()
        db.consume_job_guidance.return_value = 4
        body = messaging_module.GuidanceAckRequest(
            guidance_ids=["g1"],
            reply_keys=["id:r1"],
            feedback_keys=["feedback:id:f1"],
            delegation_keys=["delegation:id:d1"],
            checkpoint_id="cp-9",
        )
        with (
            patch.object(om.app.state.resources, "postgres_db", db),
        ):
            result = await _ack_job_guidance(om, MagicMock(), JOB_ID, body)

        assert result == {"status": "ok", "consumed": 4}
        db.consume_job_guidance.assert_awaited_once_with(
            JOB_ID,
            guidance_ids=["g1"],
            reply_threads=[],
            reply_keys=["id:r1"],
            feedback_keys=["feedback:id:f1"],
            delegation_keys=["delegation:id:d1"],
            checkpoint_id="cp-9",
        )

    @pytest.mark.asyncio
    async def test_missing_checkpoint_proof_is_conflict(self):
        import orchestrator.main as om

        db = AsyncMock()
        db.consume_job_guidance.side_effect = ValueError("checkpoint missing")
        with (
            patch.object(om.app.state.resources, "postgres_db", db),
            pytest.raises(HTTPException) as exc,
        ):
            await _ack_job_guidance(
                om,
                MagicMock(),
                JOB_ID,
                messaging_module.GuidanceAckRequest(guidance_ids=["g1"]),
            )

        assert exc.value.status_code == 409


# =============================================================================
# Heartbeat response carries pending guidance (zero marginal DB cost)
# =============================================================================


class TestHeartbeatCarriesGuidance:
    def _heartbeat_body(self, om):
        return agent_runtime_module.AgentHeartbeat(
            status="working", current_job_id=JOB_ID
        )

    def _db(self, context):
        db = AsyncMock()
        db.heartbeat.return_value = {
            "previous_status": "working",
            "effective_status": "working",
            "intents": {},
        }
        db.get_job.return_value = {
            "id": JOB_ID,
            "status": "processing",
            "context": context,
        }
        return db

    @pytest.mark.asyncio
    async def test_attaches_pending_guidance_from_job_row(self):
        import orchestrator.main as om

        entries = [{"id": "g1", "text": "steer", "source": "officer"}]
        db = self._db({"pending_guidance": entries})
        with (
            patch.object(om.app.state.resources, "postgres_db", db),
            patch.object(access_module, "require_internal", AsyncMock()),
        ):
            out = await agent_registration.agent_heartbeat(
                MagicMock(),
                "agent-1",
                self._heartbeat_body(om),
                dependencies=sessions_composition.agent_registration_dependencies(
                    om.app.state.resources
                ),
            )

        assert out["job_status"] == "processing"
        assert out["pending_guidance"] == entries

    @pytest.mark.asyncio
    async def test_row_read_without_guidance_sends_empty_list_prune_signal(self):
        import orchestrator.main as om

        db = self._db({})
        with (
            patch.object(om.app.state.resources, "postgres_db", db),
            patch.object(access_module, "require_internal", AsyncMock()),
        ):
            out = await agent_registration.agent_heartbeat(
                MagicMock(),
                "agent-1",
                self._heartbeat_body(om),
                dependencies=sessions_composition.agent_registration_dependencies(
                    om.app.state.resources
                ),
            )

        assert out["pending_guidance"] == []

    @pytest.mark.asyncio
    async def test_lookup_failure_sends_none_keep_inbox(self):
        import orchestrator.main as om

        db = self._db({})
        db.get_job.side_effect = RuntimeError("db blip")
        with (
            patch.object(om.app.state.resources, "postgres_db", db),
            patch.object(access_module, "require_internal", AsyncMock()),
        ):
            out = await agent_registration.agent_heartbeat(
                MagicMock(),
                "agent-1",
                self._heartbeat_body(om),
                dependencies=sessions_composition.agent_registration_dependencies(
                    om.app.state.resources
                ),
            )

        assert out["pending_guidance"] is None
        assert out["job_status"] is None


# =============================================================================
# dual_app inbox + ack
# =============================================================================


class TestDualAppGuidanceInbox:
    @pytest.fixture(autouse=True)
    def _restore_dual_app_globals(self):
        names = (
            "_pod_state",
            "_current_job_id",
            "_current_job_task",
            "_stop_reason",
            "_drain_intent_received",
            "_drain_intent_handled",
            "_orchestrator_client",
        )
        saved = {name: getattr(dual_app, name) for name in names}
        saved_inbox = dict(dual_app._guidance_inbox)
        stop_req = dual_app._stop_requested.is_set()

        dual_app._pod_state = dual_app.PodState.IDLE
        dual_app._current_job_id = None
        dual_app._current_job_task = None
        dual_app._drain_intent_received = False
        dual_app._drain_intent_handled = False
        dual_app._orchestrator_client = None
        dual_app._guidance_inbox.clear()
        dual_app._clear_stop()
        yield
        for name, val in saved.items():
            setattr(dual_app, name, val)
        dual_app._guidance_inbox.clear()
        dual_app._guidance_inbox.update(saved_inbox)
        (dual_app._stop_requested.set if stop_req else dual_app._stop_requested.clear)()

    def _working(self, job_id="job-under-test"):
        dual_app._pod_state = dual_app.PodState.WORKING
        dual_app._current_job_id = job_id

    @pytest.mark.asyncio
    async def test_working_pod_stores_entries(self):
        self._working()
        entries = [{"id": "g1", "text": "steer", "source": "officer"}]
        await dual_app._handle_heartbeat_intents(
            {"status": "ok", "job_status": "processing", "pending_guidance": entries}
        )
        assert dual_app.get_pending_guidance("job-under-test") == entries

    @pytest.mark.asyncio
    async def test_empty_list_prunes_inbox(self):
        """Once the ack landed, the orchestrator sends [] and the inbox clears
        — the rider stops rendering."""
        self._working()
        dual_app._guidance_inbox["job-under-test"] = [{"id": "g1", "text": "old"}]
        await dual_app._handle_heartbeat_intents(
            {"status": "ok", "job_status": "processing", "pending_guidance": []}
        )
        assert dual_app.get_pending_guidance("job-under-test") == []

    @pytest.mark.asyncio
    async def test_missing_field_keeps_inbox(self):
        """Older orchestrator / failed lookup -> no information, keep as-is."""
        self._working()
        dual_app._guidance_inbox["job-under-test"] = [{"id": "g1", "text": "keep"}]
        await dual_app._handle_heartbeat_intents(
            {"status": "ok", "job_status": "processing"}
        )
        assert dual_app.get_pending_guidance("job-under-test") == [
            {"id": "g1", "text": "keep"}
        ]

    @pytest.mark.asyncio
    async def test_idle_pod_ignores_guidance(self):
        await dual_app._handle_heartbeat_intents(
            {"status": "ok", "pending_guidance": [{"id": "g1", "text": "x"}]}
        )
        assert dual_app._guidance_inbox == {}

    @pytest.mark.asyncio
    async def test_ack_guidance_posts_ids_and_threads(self):
        client = MagicMock()
        client.ack_job_guidance = AsyncMock(return_value=True)
        dual_app._orchestrator_client = client

        dual_app.ack_guidance("job-1", guidance_ids=["g1", "g2"], reply_threads=["t1"])
        await asyncio.sleep(0)  # let the fire-and-forget task run

        client.ack_job_guidance.assert_awaited_once_with(
            "job-1", guidance_ids=["g1", "g2"], reply_threads=["t1"]
        )

    @pytest.mark.asyncio
    async def test_ack_without_client_or_ids_is_noop(self):
        dual_app.ack_guidance("job-1", guidance_ids=["g1"])  # no client
        client = MagicMock()
        client.ack_job_guidance = AsyncMock()
        dual_app._orchestrator_client = client
        dual_app.ack_guidance("job-1")  # nothing to ack
        await asyncio.sleep(0)
        client.ack_job_guidance.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_ack_failure_is_swallowed(self):
        """Best-effort: a failed ack must never propagate — the entries just
        stay pending and get redelivered."""
        client = MagicMock()
        client.ack_job_guidance = AsyncMock(side_effect=RuntimeError("down"))
        dual_app._orchestrator_client = client
        dual_app.ack_guidance("job-1", guidance_ids=["g1"])
        await asyncio.sleep(0)
        client.ack_job_guidance.assert_awaited_once()


# =============================================================================
# Rendering
# =============================================================================


class TestGuidanceRendering:
    def test_single_block_contains_all_entries(self):
        block = format_supervisor_guidance(
            [
                {"id": "a", "text": "do X", "source": "officer"},
                {"id": "b", "text": "then Y", "source": "officer"},
            ]
        )
        assert block.count("[SUPERVISOR GUIDANCE]") == 1
        assert "do X" in block and "then Y" in block

    def test_empty_or_blank_entries_render_nothing(self):
        assert format_supervisor_guidance([]) == ""
        assert format_supervisor_guidance([{"id": "a", "text": "  "}]) == ""


# =============================================================================
# The execute-node rider (real wiring: inbox -> render -> single block -> ack)
# =============================================================================


class TestExecuteRendersGuidance:
    def _make_execute(
        self,
        workspace_manager,
        todo_manager,
        captured_requests,
        *,
        tool_context=None,
    ):
        from agent.graph import create_execute_node

        async def fake_ainvoke(prepared, **kwargs):
            captured_requests.append(list(prepared))
            return AIMessage(content="ok")

        llm = MagicMock()
        llm.ainvoke = AsyncMock(side_effect=fake_ainvoke)

        config = MagicMock()
        config.agent_id = "test-agent"
        config.extra = {}
        config.llm.model = "test-model"
        # U1: the execute node reads the single config.llm directly (one model
        # for every phase, no per-phase resolution), so the fields the node
        # consults live on it rather than on a get_phase_config() result.
        config.llm.timeout = 10.0
        config.llm.model_max_context_tokens = 100000
        config.limits.model_max_context_tokens = 100000
        config.limits.response_validation.enabled = False
        config.context_management.max_summary_length = 500

        context_mgr = MagicMock()
        context_mgr.get_token_count.return_value = 50
        context_mgr.config.compaction_threshold_tokens = 100000
        context_mgr.config.summarization_threshold_tokens = 100000
        context_mgr.config.keep_recent_messages = 10
        context_mgr.ensure_within_limits = AsyncMock(
            side_effect=lambda msgs, *a, **k: msgs
        )
        context_mgr.clear_old_tool_results.side_effect = lambda msgs: msgs

        return create_execute_node(
            llm_with_tools=llm,
            todo_manager=todo_manager,
            memory_manager=MagicMock(),
            workspace_manager=workspace_manager,
            config=config,
            context_mgr=context_mgr,
            retry_manager=MagicMock(),
            auxiliary_llm=None,
            summarization_prompt="",
            tool_context=tool_context,
            tool_names=["read_file"],
        )

    def _state(self):
        return {
            "job_id": "job-under-test",
            "iteration": 1,
            "messages": [HumanMessage(content="hello")],
            "is_strategic_phase": False,
            "phase_number": 2,
            "metadata": {},
        }

    @pytest.fixture(autouse=True)
    def _clean_inbox(self):
        saved = dict(dual_app._guidance_inbox)
        dual_app._guidance_inbox.clear()
        yield
        dual_app._guidance_inbox.clear()
        dual_app._guidance_inbox.update(saved)

    @pytest.mark.asyncio
    async def test_renders_one_block_with_all_entries_and_acks(
        self, workspace_manager, todo_manager
    ):
        dual_app._guidance_inbox["job-under-test"] = [
            {"id": "g1", "text": "stop retrying X", "source": "officer"},
            {"id": "g2", "text": "read file Z", "source": "officer"},
        ]
        captured = []
        execute = self._make_execute(workspace_manager, todo_manager, captured)
        ack = MagicMock()

        with (
            patch("agent.graph.get_phase_system_prompt", return_value="SYS"),
            patch("agent.graph.get_phase_system_prompt", return_value="SYS"),
            patch("agent.graph.get_archiver", return_value=None),
            patch.object(dual_app, "ack_guidance", ack),
        ):
            # The tail of execute (response validation, reminders) is not
            # under test; the request build + ack both happen before it.
            with contextlib.suppress(Exception):
                await execute(self._state())

        assert captured, "LLM was never invoked"
        request = captured[0]
        blocks = [
            m
            for m in request
            if isinstance(getattr(m, "content", ""), str)
            and "[SUPERVISOR GUIDANCE]" in m.content
        ]
        assert len(blocks) == 1, "expected exactly one [SUPERVISOR GUIDANCE] block"
        assert "stop retrying X" in blocks[0].content
        assert "read file Z" in blocks[0].content

        ack.assert_called_once_with(
            "job-under-test", guidance_ids=["g1", "g2"], reply_threads=None
        )

    @pytest.mark.asyncio
    async def test_empty_inbox_renders_nothing_and_never_acks(
        self, workspace_manager, todo_manager
    ):
        captured = []
        execute = self._make_execute(workspace_manager, todo_manager, captured)
        ack = MagicMock()

        with (
            patch("agent.graph.get_phase_system_prompt", return_value="SYS"),
            patch("agent.graph.get_phase_system_prompt", return_value="SYS"),
            patch("agent.graph.get_archiver", return_value=None),
            patch.object(dual_app, "ack_guidance", ack),
        ):
            with contextlib.suppress(Exception):
                await execute(self._state())

        assert captured
        assert not any(
            "[SUPERVISOR GUIDANCE]" in getattr(m, "content", "")
            for m in captured[0]
            if isinstance(getattr(m, "content", ""), str)
        )
        ack.assert_not_called()

    @pytest.mark.asyncio
    async def test_stateless_guidance_is_checkpointed_before_ack(
        self, workspace_manager, todo_manager
    ):
        from agent.tools.context import ToolContext

        dual_app._guidance_inbox["job-under-test"] = [
            {"id": "g1", "text": "read file Z", "source": "officer"}
        ]
        context = ToolContext(workspace_manager=workspace_manager)
        context._stateless_worker = True
        captured = []
        execute = self._make_execute(
            workspace_manager,
            todo_manager,
            captured,
            tool_context=context,
        )
        ack = MagicMock()

        with (
            patch("agent.graph.get_phase_system_prompt", return_value="SYS"),
            patch("agent.graph.get_phase_system_prompt", return_value="SYS"),
            patch("agent.graph.get_archiver", return_value=None),
            patch.object(dual_app, "ack_guidance", ack),
        ):
            result = await execute(self._state())

        assert captured
        assert result["delivered_guidance_ids"] == ["g1"]
        ack.assert_not_called()

    @pytest.mark.asyncio
    async def test_stateless_reclaim_suppresses_checkpointed_guidance(
        self, workspace_manager, todo_manager
    ):
        from agent.tools.context import ToolContext

        dual_app._guidance_inbox["job-under-test"] = [
            {"id": "g1", "text": "already absorbed", "source": "officer"}
        ]
        context = ToolContext(workspace_manager=workspace_manager)
        context._stateless_worker = True
        captured = []
        execute = self._make_execute(
            workspace_manager,
            todo_manager,
            captured,
            tool_context=context,
        )
        state = self._state()
        state["delivered_guidance_ids"] = ["g1"]

        with (
            patch("agent.graph.get_phase_system_prompt", return_value="SYS"),
            patch("agent.graph.get_phase_system_prompt", return_value="SYS"),
            patch("agent.graph.get_archiver", return_value=None),
        ):
            await execute(state)

        assert captured
        assert not any(
            "[SUPERVISOR GUIDANCE]" in getattr(message, "content", "")
            for message in captured[0]
        )


# =============================================================================
# Queued lane: drained replies reach visible context and clear
# =============================================================================


class TestQueuedReplyDrain:
    def _node(self, managers_ws, todo_mgr, mock_config, postgres_db):
        from agent.graph import create_handle_transition_node

        phase_settings = MagicMock()
        phase_settings.min_todos = 5
        phase_settings.max_todos = 20
        mock_config.phase_settings = phase_settings
        return create_handle_transition_node(
            managers_ws,
            todo_mgr,
            mock_config,
            min_todos=5,
            max_todos=20,
            postgres_db=postgres_db,
        )

    def _tactical_state(self):
        return {
            "job_id": "test-123",
            "is_strategic_phase": False,
            "phase_number": 1,
            "iteration": 20,
        }

    @pytest.mark.asyncio
    async def test_drain_injects_visible_message_and_acks_exact_reply(
        self, workspace_manager, todo_manager, mock_config
    ):
        from shared.job_steering import queued_reply_key

        todo_manager.add("Task 1")
        todo_manager.complete("todo_1")

        reply = {
            "thread_id": "officer",
            "message": "prioritize the report",
            "timestamp": "2026-07-30T10:00:00Z",
        }
        db = AsyncMock()
        db.fetchrow.return_value = {"context": {"queued_replies": [reply]}}
        node = self._node(workspace_manager, todo_manager, mock_config, db)

        ack = MagicMock()
        with patch("agent.graph._ack_supervisor_guidance", ack):
            result = await node(self._tactical_state())

        contents = [m.content for m in result["messages"]]
        queued_blocks = [c for c in contents if "[QUEUED MESSAGES]" in c]
        assert len(queued_blocks) == 1, "drained reply must reach visible context"
        assert "prioritize the report" in queued_blocks[0]

        # audit file still written
        assert workspace_manager.exists("messages/officer/001_received.md")

        ack.assert_called_once_with("test-123", reply_keys=[queued_reply_key(reply)])

    @pytest.mark.asyncio
    async def test_cleared_context_does_not_rematerialize(
        self, workspace_manager, todo_manager, mock_config
    ):
        """After the ack moved the thread to consumed_replies, the next
        boundary sees no queued_replies -> no duplicate injection."""
        todo_manager.add("Task 1")
        todo_manager.complete("todo_1")

        db = AsyncMock()
        db.fetchrow.return_value = {"context": {"consumed_replies": [{"id": "x"}]}}
        node = self._node(workspace_manager, todo_manager, mock_config, db)

        ack = MagicMock()
        with patch("agent.graph._ack_supervisor_guidance", ack):
            result = await node(self._tactical_state())

        assert not any("[QUEUED MESSAGES]" in m.content for m in result["messages"])
        ack.assert_not_called()
        assert not workspace_manager.exists("messages/officer/001_received.md")


# =============================================================================
# [FEEDBACK_RESUME] banner honesty + in-flight todo archiving
# =============================================================================


class TestFeedbackResume:
    def _node(self, workspace_manager, todo_manager, mock_config):
        from agent.graph import create_restore_from_feedback_node

        context_mgr = MagicMock()
        context_mgr.ensure_within_limits = AsyncMock(
            side_effect=lambda msgs, *a, **k: msgs
        )
        return create_restore_from_feedback_node(
            workspace_manager,
            todo_manager,
            mock_config,
            context_mgr,
            auxiliary_llm=None,
            summarization_prompt="",
        )

    def _base_state(self, **extra):
        state = {
            "job_id": "test-123",
            "resume_feedback": "fix finding F1",
            "messages": [HumanMessage(content="old context")],
            "phase_number": 2,
            "is_strategic_phase": False,
        }
        state.update(extra)
        return state

    @pytest.mark.asyncio
    async def test_banner_reflects_passed_reason(
        self, workspace_manager, todo_manager, mock_config
    ):
        node = self._node(workspace_manager, todo_manager, mock_config)
        reason = (
            "The critic reviewed the completed work and returned it with "
            "open findings; address them."
        )
        with patch("agent.graph.get_resume_strategic_todos", return_value=[]):
            result = await node(self._base_state(resume_reason=reason))

        banner = result["messages"][-1].content
        assert banner.startswith(f"[FEEDBACK_RESUME] {reason}")
        assert "previously frozen for human review" not in banner
        assert "fix finding F1" in banner
        # consumed
        assert result["resume_reason"] is None
        assert result["resume_feedback"] is None
        # feedback.md carries the same cause
        assert reason in workspace_manager.read_file("feedback.md")

    @pytest.mark.asyncio
    async def test_banner_generic_fallback_makes_no_false_claim(
        self, workspace_manager, todo_manager, mock_config
    ):
        node = self._node(workspace_manager, todo_manager, mock_config)
        with patch("agent.graph.get_resume_strategic_todos", return_value=[]):
            result = await node(self._base_state())

        banner = result["messages"][-1].content
        assert banner.startswith("[FEEDBACK_RESUME]")
        assert "previously frozen for human review" not in banner
        assert "resumed with feedback" in banner

    @pytest.mark.asyncio
    async def test_inflight_todos_archived_with_preemption_note(
        self, workspace_manager, todo_manager, mock_config
    ):
        """The silent wipe is gone: checkpointed in-flight todos land in an
        archive file with an honest preemption note before the resume todos
        replace them."""
        node = self._node(workspace_manager, todo_manager, mock_config)
        state = self._base_state(
            todos=[
                {
                    "id": "todo_1",
                    "content": "half-done tactical step",
                    "status": "in_progress",
                },
                {
                    "id": "todo_2",
                    "content": "unstarted tactical step",
                    "status": "pending",
                },
            ],
            todo_next_id=3,
        )
        with patch("agent.graph.get_resume_strategic_todos", return_value=[]):
            await node(state)

        archive_files = workspace_manager.list_files("archive")
        assert archive_files, "no archive written for preempted todos"
        archive_name = Path(sorted(archive_files)[0]).name
        content = workspace_manager.read_file(f"archive/{archive_name}")
        assert "Preempted by Feedback Resume" in content
        assert "half-done tactical step" in content
        assert "unstarted tactical step" in content
        assert "in flight, not failed" in content

    @pytest.mark.asyncio
    async def test_no_inflight_todos_no_archive(
        self, workspace_manager, todo_manager, mock_config
    ):
        node = self._node(workspace_manager, todo_manager, mock_config)
        with patch("agent.graph.get_resume_strategic_todos", return_value=[]):
            await node(self._base_state())

        try:
            archive_files = workspace_manager.list_files("archive")
        except Exception:
            archive_files = []
        assert not archive_files
