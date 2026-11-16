"""``UniversalAgent._create_phase_llms`` after the U1 tier collapse.

One ``llm.model`` builds ONE client that every phase shares; the only way to get
a second client is an ``llm.summarization`` override that resolves to a
different config. Ported from the retired ``tests/test_phase_model_budget.py``
(the per-phase window/params reconciliation went with the tiers).
"""

from types import SimpleNamespace
from unittest.mock import patch

from shared.runtime.core.loader import (
    _apply_settings_matrix,
    load_agent_config_from_dict,
)

GEMMA = "RedHatAI/gemma-4-31B-it-FP8-Dynamic"  # base default
GPT55 = "gpt-5.5"
GPT54MINI = "gpt-5.4-mini"


def _make_config(llm_overrides: dict):
    """Build an AgentConfig the way the loader does: base gemma with the settings
    matrix applied, plus the given llm keys."""
    data = {
        "agent_id": "test-agent",
        "display_name": "Test Agent",
        "llm": {"model": GEMMA, **llm_overrides},
    }
    _apply_settings_matrix(data, set(), None)
    return load_agent_config_from_dict(data)


class _FakeAgent:
    """Minimal shell exposing only what _create_phase_llms touches."""

    def __init__(self, config):
        self.config = config
        self._strategic_llm = None
        self._tactical_llm = None
        self._summarization_llm = None
        self._llm = None

    def _initialize_auxiliary_llm(self, *a, **k):
        pass

    def _initialize_citation_verifier(self, *a, **k):
        pass


def _run_create_phase_llms(config):
    from agent.agent import UniversalAgent

    fake = _FakeAgent(config)
    created = []

    def fake_create_llm(cfg, limits=None):
        created.append(cfg)
        return SimpleNamespace(model=cfg.model, cfg=cfg)

    with patch("agent.agent.create_llm", side_effect=fake_create_llm):
        UniversalAgent._create_phase_llms(fake)
    return fake, created


class TestCreatePhaseLLMsSingleModel:
    def test_session_shape_unchanged(self):
        config = _make_config({})
        before = config.limits.model_max_context_tokens
        fake, created = _run_create_phase_llms(config)
        assert fake.config.limits.model_max_context_tokens == before
        assert fake._model_config_warnings == []
        assert len(created) == 1  # one client for every phase
        assert fake._llm is fake._strategic_llm is fake._tactical_llm
        assert fake._summarization_llm is fake._llm
        assert created[0].model == GEMMA

    def test_legacy_phase_pins_collapse_to_one_client(self):
        """A hand-built pre-U1 dict with strategic/tactical pins runs ONE model
        (the strategic one, per the merged-dict rule) — no second client."""
        config = _make_config(
            {"strategic": {"model": GPT55}, "tactical": {"model": GPT54MINI}}
        )
        assert config.llm.model == GPT55
        fake, created = _run_create_phase_llms(config)
        assert [c.model for c in created] == [GPT55]
        assert fake._strategic_llm is fake._tactical_llm

    def test_summarization_override_creates_second_client(self):
        config = _make_config({"summarization": {"model": GPT54MINI}})
        fake, created = _run_create_phase_llms(config)
        assert [c.model for c in created] == [GEMMA, GPT54MINI]
        assert fake._summarization_llm is not fake._llm
        assert fake._summarization_llm.cfg.model == GPT54MINI
        # The resolved summarization client carries no phase override itself.
        assert fake._summarization_llm.cfg.summarization is None

    def test_identical_summarization_dedupes(self):
        """An override that resolves to the main model's config reuses the
        client instead of opening a second connection to the same endpoint."""
        config = _make_config({"summarization": {"model": GEMMA}})
        fake, created = _run_create_phase_llms(config)
        assert len(created) == 1
        assert fake._summarization_llm is fake._llm

    def test_summarization_params_only_override_still_distinct(self):
        config = _make_config({"summarization": {"temperature": 0.9}})
        fake, created = _run_create_phase_llms(config)
        assert len(created) == 2
        assert fake._summarization_llm.cfg.temperature == 0.9


# ---------------------------------------------------------------------------
# ``_bind_job_tools`` (U2): one stable union binding for every phase
# ---------------------------------------------------------------------------

from langchain_core.tools import StructuredTool  # noqa: E402

from shared.runtime.core.loader import (  # noqa: E402
    InstructionFileEntry,
    supports_parallel_tool_calls,
)
from agent.tools.context import ToolContext  # noqa: E402
from agent.tools.registry import apply_instruction_enforcement  # noqa: E402

_BIND_TOOLS = ("job_complete", "request_replan", "read_file", "todo_complete")


def _structured(name: str) -> StructuredTool:
    return StructuredTool.from_function(
        func=lambda: "ok", name=name, description=f"Does {name}."
    )


class _RecordingLLM:
    """``bind_tools`` returns a distinct object per call and records the schema
    it was handed (names, descriptions, kwargs)."""

    def __init__(self):
        self.calls = []

    def bind_tools(self, tools, **kwargs):
        bound = SimpleNamespace(
            tool_names=[t.name for t in tools],
            descriptions={t.name: t.description for t in tools},
            kwargs=kwargs,
        )
        self.calls.append(bound)
        return bound


class _BindAgent:
    """Only what ``_bind_job_tools`` touches."""

    def __init__(self, config, tools):
        self.config = config
        self._tools = tools
        self._llm = self._strategic_llm = self._tactical_llm = _RecordingLLM()
        self._llm_with_tools = None
        self._strategic_llm_with_tools = None
        self._tactical_llm_with_tools = None


def _bind(config, tools) -> _BindAgent:
    from agent.agent import UniversalAgent

    agent = _BindAgent(config, tools)
    UniversalAgent._bind_job_tools(agent)
    return agent


class TestBindJobTools:
    def test_binds_the_union_once(self):
        config = _make_config({})
        tools = [_structured(n) for n in _BIND_TOOLS]
        agent = _bind(config, tools)

        (bound,) = agent._llm.calls
        assert sorted(bound.tool_names) == sorted(_BIND_TOOLS)
        assert agent._llm_with_tools is bound
        assert (
            agent._strategic_llm_with_tools
            is agent._tactical_llm_with_tools
            is agent._llm_with_tools
        )
        # Single-phase tools state their phase in the bound description;
        # both-phase tools do not (the family Examples block the guardrails
        # append for gemma's read_file is why these are prefix checks).
        assert bound.descriptions["job_complete"].startswith(
            "[strategic-phase tool] Does job_complete."
        )
        assert bound.descriptions["request_replan"].startswith(
            "[tactical-phase tool] Does request_replan."
        )
        assert bound.descriptions["read_file"].startswith("Does read_file.")
        assert "-phase tool]" not in bound.descriptions["read_file"]
        # ... on the bound copies only: the ToolNode's objects are untouched.
        assert agent._tools is tools
        assert [t.description for t in tools] == [f"Does {n}." for n in _BIND_TOOLS]
        # The parallel_tool_calls gate is applied once, from the config.
        llm_cfg = config.llm
        expected = (
            {"parallel_tool_calls": llm_cfg.parallel_tool_calls}
            if supports_parallel_tool_calls(llm_cfg.provider, llm_cfg.model)
            else {}
        )
        assert bound.kwargs == expected

    def test_job_complete_without_todo_complete_says_it_ends_the_job(self):
        """A job whose tools do not include todo_complete closes its one task
        with job_complete: the bound copy gets the description that says the
        call ends the job (no phase prefix, no remaining todos)."""
        from agent.tools.core.job import JOB_COMPLETE_DESCRIPTION_WITHOUT_TODO_TOOL

        names = ("job_complete", "read_file")
        tools = [_structured(n) for n in names]
        agent = _bind(_make_config({}), tools)

        (bound,) = agent._llm.calls
        assert (
            bound.descriptions["job_complete"]
            == JOB_COMPLETE_DESCRIPTION_WITHOUT_TODO_TOOL
        )
        assert bound.descriptions["read_file"].startswith("Does read_file.")
        # The ToolNode's object keeps its own description.
        assert tools[0].description == "Does job_complete."

    def test_unregistered_tools_are_not_bound(self):
        agent = _bind(
            _make_config({}),
            [_structured("read_file"), _structured("custom_unregistered")],
        )
        (binding,) = agent._llm.calls
        assert "custom_unregistered" not in binding.tool_names
        assert "read_file" in binding.tool_names

    def test_before_tool_enforcement_survives_binding(self):
        """The todo-guide gate wraps the ToolNode's tool objects in place;
        binding copies for the schema never undo it."""
        ctx = ToolContext()
        ctx._instruction_files = [
            InstructionFileEntry(
                trigger="before_tool:next_phase_todos",
                skill="todo-guide",
                enforce=True,
            )
        ]
        tools = apply_instruction_enforcement(
            [_structured("next_phase_todos"), _structured("read_file")], ctx
        )
        gate = tools[0].func
        ctx.set_current_phase("strategic", phase_number=1, turn_count=1)
        assert "skills/todo-guide/SKILL.md" in gate()  # closed: guide unread

        agent = _bind(_make_config({}), tools)
        assert agent._tools[0].func is gate  # what the ToolNode executes
        assert "next_phase_todos" in agent._llm.calls[0].tool_names


# ---------------------------------------------------------------------------
# ``_install_subagent_runtime`` (U3 WP2): the WorkerHost + runtime per job
# ---------------------------------------------------------------------------


def _delegating_context(**config_overrides) -> ToolContext:
    config = {
        "agent_id": "developer",
        "delegation": {"enabled": True, "max_concurrent": 3},
        "subagents": {
            "default": "explorer",
            "roster": {
                "explorer": {"agent_id": "explorer", "display_name": "Explorer"}
            },
        },
    }
    config.update(config_overrides)
    return ToolContext(
        config=config,
        _job_metadata={"job_id": "job-42", "config_name": "developer"},
    )


class _HostAgent:
    """Only what _install_subagent_runtime touches."""

    def __init__(self, config):
        self.config = config
        self._auxiliary_llm = object()
        self.postgres_conn = object()
        self._current_job_id = "job-42"


def _install(ctx, agent=None):
    from agent.agent import UniversalAgent

    agent = agent or _HostAgent(_make_config({}))
    UniversalAgent._install_subagent_runtime(agent, ctx)
    return agent


class TestSubagentHostWiring:
    def test_stateless_authority_is_captured_before_the_context_lease_stamp(self):
        job_id = "aaaaaaaa-1111-4222-8333-444444444444"
        ctx = _delegating_context()
        agent = _HostAgent(_make_config({}))
        agent._current_job_id = job_id
        agent._worker_lease_token = 41

        _install(ctx, agent)

        authority = ctx._parent_execution_authority
        assert authority.execution_lane == "stateless"
        assert str(authority.parent_job_id) == job_id
        assert authority.worker_lease_token == 41
        assert ctx._worker_lease_token is None

    def test_pinned_authority_captures_registered_process_and_pod(self):
        job_id = "aaaaaaaa-1111-4222-8333-444444444444"
        agent_id = "bbbbbbbb-1111-4222-8333-444444444444"
        ctx = _delegating_context()
        ctx.orchestrator_client = SimpleNamespace(
            agent_id=agent_id,
            dispatch_process_generation="process-7",
        )
        agent = _HostAgent(_make_config({}))
        agent._current_job_id = job_id
        with patch.dict("os.environ", {"POD_UID": "pod-7"}):
            _install(ctx, agent)

        authority = ctx._parent_execution_authority
        assert authority.execution_lane == "pinned"
        assert str(authority.parent_job_id) == job_id
        assert str(authority.agent_id) == agent_id
        assert authority.pod_uid == "pod-7"
        assert authority.dispatch_process_generation == "process-7"

    def test_enabled_delegation_installs_the_host_and_the_runtime(self):
        from agent.subagents import SubagentRuntime, WorkerHost

        ctx = _delegating_context()
        agent = _install(ctx)
        runtime = ctx.subagent_runtime
        host = ctx._parent_host
        assert isinstance(runtime, SubagentRuntime)
        assert isinstance(host, WorkerHost)
        assert runtime.host is host and runtime.parent_context is ctx
        assert host.job_id == "job-42"
        assert host.agent_type == agent.config.agent_id
        assert host.thread_id is None
        assert host.auxiliary_llm is agent._auxiliary_llm
        assert host.live_llm_config is agent.config.llm
        assert host.postgres is agent.postgres_conn
        assert host.tool_context is ctx
        assert ctx.auxiliary_llm is agent._auxiliary_llm
        assert runtime.max_concurrent == 3
        assert runtime.roster_names == ["explorer"]
        assert runtime.default == "explorer"

    def test_the_admission_fence_follows_the_graphs_drain_seam(self):
        ctx = _delegating_context()
        with patch("agent.graph._is_drain_requested", return_value=False) as drain:
            _install(ctx)
            host = ctx._parent_host
            assert callable(ctx.provider_admission)
            assert ctx.provider_admission() is True
            assert host.provider_admission() is True
            drain.return_value = True
            assert ctx.provider_admission() is False
            assert host.provider_admission() is False

    def test_audit_metadata_prefers_the_batch_stamp(self):
        ctx = _delegating_context()
        _install(ctx)
        host = ctx._parent_host
        assert host.audit_metadata == {"job_id": "job-42", "config_name": "developer"}
        stamped = {"job_id": "job-42", "document_path": "/data/doc.pdf"}
        ctx._parent_audit_metadata = stamped
        assert host.audit_metadata is stamped

    def test_probe_and_fork_source_are_empty_until_the_graph_stamps_them(self):
        from agent.subagents import ContextProbe

        ctx = _delegating_context()
        _install(ctx)
        host = ctx._parent_host
        assert host.context_probe() is None
        assert host.fork_source() == []
        probe = ContextProbe(None, 10, 100, 1000)
        ctx.parent_context_probe = lambda: probe
        messages = [object(), object()]
        ctx._fork_source = messages
        assert host.context_probe() is probe
        assert host.fork_source() == messages
        assert host.fork_source() is not messages  # a copy, never the live list

    def test_the_ledger_is_the_db_one_when_the_context_carries_both_halves(self):
        """WP3: durable child rows need the orchestrator client (row creation)
        and the agent-side pool (transcript + lifecycle); with either missing
        the runtime keeps the null ledger and a child leaves no trace."""
        from shared.subagent_parent_authority import ParentExecutionAuthority
        from agent.subagents import DbSubagentLedger, NullLedger

        ctx = _delegating_context()
        _install(ctx)
        assert isinstance(ctx.subagent_runtime.ledger, NullLedger)

        client, pool = object(), object()
        wired = _delegating_context()
        wired.orchestrator_client = client
        wired.postgres_db = pool
        wired._parent_execution_authority = ParentExecutionAuthority(
            execution_lane="stateless",
            parent_job_id="aaaaaaaa-1111-4222-8333-444444444444",
            worker_lease_token=7,
        )
        _install(wired)
        ledger = wired.subagent_runtime.ledger
        assert isinstance(ledger, DbSubagentLedger)
        assert ledger.client is client and ledger.postgres is pool
        assert ledger.parent_context is wired

        half = _delegating_context()
        half.orchestrator_client = client
        _install(half)
        assert isinstance(half.subagent_runtime.ledger, NullLedger)

    def test_disabled_delegation_installs_no_runtime_but_still_the_stashes(self):
        ctx = _delegating_context(delegation={"enabled": False})
        agent = _install(ctx)
        assert ctx.subagent_runtime is None
        assert ctx._parent_host is None
        assert ctx.auxiliary_llm is agent._auxiliary_llm
        assert callable(ctx.provider_admission)

    def test_an_install_failure_is_non_fatal(self):
        ctx = _delegating_context()
        with patch(
            "agent.subagents.runtime.SubagentRuntime.from_context",
            side_effect=RuntimeError("boom"),
        ):
            _install(ctx)
        assert ctx.subagent_runtime is None

    def test_the_tool_builds_a_runtime_lazily_when_none_was_installed(self):
        from agent.subagents import SubagentRuntime, WorkerHost
        from agent.tools.delegation.delegate_agent import ensure_runtime

        ctx = _delegating_context()
        ctx._llm_config = _make_config({}).llm
        runtime = ensure_runtime(ctx)
        assert isinstance(runtime, SubagentRuntime)
        assert ctx.subagent_runtime is runtime
        assert isinstance(ctx._parent_host, WorkerHost)
        assert ctx._parent_host.job_id == "job-42"
        assert ctx._parent_host.agent_type == "developer"
        assert ctx._parent_host.live_llm_config is ctx._llm_config
        assert ensure_runtime(ctx) is runtime
