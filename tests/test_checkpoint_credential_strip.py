"""Connector credentials never become graph state or reach a checkpoint.

Job metadata carries the dispatch ``datasources`` payload with plaintext
``credentials`` (Git tokens, SSH keys, env values, credential-file contents)
and the project ``repositories`` payload with its own ``credentials``.
``create_initial_state`` stores metadata as the graph's ``metadata`` channel
and the checkpointer persists it, so every job entry path strips those
secrets first (``checkpoint_safe_metadata``). The runtime keeps reading the
credentials from the dispatch payload in process memory.

Sessions have no graph state and no checkpointer (``persistent_graph`` is a
plain loop), so the job paths below are every entry into graph state.

See knowledge-base/knowledge/issues/
connector_credentials_persist_in_checkpoints_and_snapshots.md.
"""

from __future__ import annotations

import copy
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch
from uuid import uuid4

import pytest
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, StateGraph

from agent.agent import UniversalAgent
from agent.core.state import (
    UniversalAgentState,
    checkpoint_safe_metadata,
    create_initial_state,
)
from shared.runtime.core.loader import AgentConfig

TOKEN = "c0-token-7f3a9e1b5d2c"
SSH_KEY_BODY = "b3BlbnNzaC1rZXktdjEAAAAAc0SshRepoKeyBody9d41"
SSH_KEY = (
    "-----BEGIN OPENSSH PRIVATE KEY-----\n"
    f"{SSH_KEY_BODY}\n"
    "-----END OPENSSH PRIVATE KEY-----\n"
)
ENV_SECRET = "c0-env-value-61c0de2a"
FILE_KEY = "c0-ssh-key-file-contents-0a77"
KUBECONFIG = "c0-kubeconfig-token-55e1"
PG_PASSWORD = "c0-pg-password-2b9f"
MCP_SECRET = "c0-mcp-bearer-80d4"
REPO_TOKEN = "c0-project-repo-token-3e1c"
MANAGED_KEY = "c0-managed-deploy-key-91aa"
QUERY_SECRET = "c0-query-api-key-77b2"
SLASH_PASSWORD = "c0/pass#word-4d10"
IDENTITY_KEY = "c0-workspace-ssh-identity-0c3e"
ACCESS_CREDENTIAL = "c0-actor-access-credential-5a5a"
REFRESH_CREDENTIAL = "c0-actor-refresh-credential-6b6b"

SECRETS = (
    TOKEN,
    SSH_KEY_BODY,
    ENV_SECRET,
    FILE_KEY,
    KUBECONFIG,
    PG_PASSWORD,
    MCP_SECRET,
    REPO_TOKEN,
    MANAGED_KEY,
    QUERY_SECRET,
    SLASH_PASSWORD,
    IDENTITY_KEY,
    ACCESS_CREDENTIAL,
    REFRESH_CREDENTIAL,
)


def _metadata() -> dict:
    """A dispatch metadata dict shaped like ``build_datasources_payload``."""
    return {
        "description": "C0 credential strip fixture",
        "project_id": "11111111-1111-1111-1111-111111111111",
        "datasources": [
            {
                "type": "repository",
                "name": "c0-token-repo",
                "description": None,
                "connection_url": "https://git.example.test/owner/repo.git",
                "credentials": {"auth_method": "token", "token": TOKEN},
                "project_read_only": False,
                "datasource_id": "22222222-2222-2222-2222-222222222222",
                "config": {"forge": "gitea"},
                "default_branch": "main",
            },
            {
                "type": "repository",
                "name": "c0-ssh-repo",
                "connection_url": "git@git.example.test:owner/other.git",
                "credentials": {"auth_method": "ssh", "ssh_key": SSH_KEY},
                "project_read_only": True,
                "config": {"forge": "github"},
            },
            {
                "type": "generic",
                "name": "c0-env",
                "connection_url": None,
                "credentials": {"env_vars": {"C0_GATE_SECRET": ENV_SECRET}},
                "project_read_only": False,
                "cli_hint": "curl with $C0_GATE_SECRET",
            },
            {
                "type": "ssh_key",
                "name": "c0-ssh-key",
                "credentials": {
                    "files": [
                        {
                            "target_path": "~/.ssh/c0-ssh-key",
                            "contents": FILE_KEY,
                            "mode": "0600",
                        }
                    ]
                },
                "project_read_only": False,
            },
            {
                "type": "kubeconfig",
                "name": "c0-kube",
                "credentials": {
                    "files": [{"target_path": "~/.kube/c0", "contents": KUBECONFIG}]
                },
                "project_read_only": False,
            },
            {
                "type": "postgresql",
                "name": "c0-db",
                "connection_url": (
                    f"postgresql://app:{PG_PASSWORD}@db.example.test:5432/app"
                ),
                "credentials": {"password": PG_PASSWORD},
                "project_read_only": True,
            },
            {
                "type": "mcp",
                "name": "c0-mcp",
                "connection_url": f"https://mcp.example.test/sse?api_key={QUERY_SECRET}",
                "credentials": {
                    "transport": "http",
                    "headers": {"Authorization": f"Bearer {MCP_SECRET}"},
                },
                "project_read_only": False,
            },
            {
                # An unencoded "/" and "#" defeat URL parsing of the userinfo.
                "type": "mongodb",
                "name": "c0-mongo",
                "connection_url": f"mongodb://app:{SLASH_PASSWORD}@mongo.example.test/db",
                "credentials": {},
                "project_read_only": True,
            },
        ],
        "repositories": [
            {
                "id": "33333333-3333-3333-3333-333333333333",
                "name": "aux",
                "role": "source",
                "repo_url": f"https://oauth2:{REPO_TOKEN}@git.example.test/o/aux.git",
                "read_only": True,
                "branch": "main",
                "credentials": {"token": REPO_TOKEN},
                "is_managed": False,
            }
        ],
        "managed_repository_credentials": [{"private_key": MANAGED_KEY}],
        "workspace_ssh_identities": [{"private_key": IDENTITY_KEY}],
        "runtime_actor": {
            "caller_kind": "worker",
            "project_id": "11111111-1111-1111-1111-111111111111",
            "user_id": "44444444-4444-4444-4444-444444444444",
            "access_credential": ACCESS_CREDENTIAL,
            "refresh_credential": REFRESH_CREDENTIAL,
        },
    }


def _assert_no_secret(blob: str | bytes) -> None:
    for secret in SECRETS:
        needle = secret.encode() if isinstance(blob, bytes) else secret
        assert needle not in blob, f"credential {secret[:10]}... persisted"


class TestCheckpointSafeMetadata:
    def test_strips_every_connector_secret(self):
        safe = checkpoint_safe_metadata(_metadata())

        _assert_no_secret(json.dumps(safe))
        assert "managed_repository_credentials" not in safe
        assert "workspace_ssh_identities" not in safe
        for entry in [*safe["datasources"], *safe["repositories"]]:
            assert not {"credentials", "connection_url", "repo_url"} & set(entry)

    def test_runtime_actor_keeps_its_identity_only(self):
        safe = checkpoint_safe_metadata(_metadata())

        assert safe["runtime_actor"] == {
            "caller_kind": "worker",
            "project_id": "11111111-1111-1111-1111-111111111111",
            "project_role": None,
            "thread_id": None,
            "officer_incarnation": None,
            "user_id": "44444444-4444-4444-4444-444444444444",
        }

    def test_entries_keep_only_identity_and_descriptive_fields(self):
        source = _metadata()
        safe = checkpoint_safe_metadata(source)

        assert safe["datasources"][0] == {
            "type": "repository",
            "name": "c0-token-repo",
            "description": None,
            "project_read_only": False,
            "datasource_id": "22222222-2222-2222-2222-222222222222",
        }
        assert safe["repositories"] == [
            {
                "id": "33333333-3333-3333-3333-333333333333",
                "name": "aux",
                "role": "source",
                "read_only": True,
            }
        ]
        assert [ds["name"] for ds in safe["datasources"]] == [
            ds["name"] for ds in source["datasources"]
        ]
        assert [ds["type"] for ds in safe["datasources"]] == [
            ds["type"] for ds in source["datasources"]
        ]
        assert safe["datasources"][1]["project_read_only"] is True
        assert safe["description"] == source["description"]
        assert safe["project_id"] == source["project_id"]

    def test_input_keeps_its_credentials(self):
        """The runtime shares these dicts; the strip must copy, not mutate."""
        source = _metadata()
        before = copy.deepcopy(source)

        checkpoint_safe_metadata(source)

        assert source == before
        assert source["datasources"][0]["credentials"]["token"] == TOKEN

    @pytest.mark.parametrize(
        "url",
        [
            "https://user:pw@host/x",
            "https://host/x?password=c0-query-secret",
            "postgresql://app:pa/ss#word@db/app",
            "app:secret@db.example.test/app",
            "deploy@host:o/r.git",
        ],
    )
    def test_urls_never_reach_state(self, url):
        safe = checkpoint_safe_metadata(
            {
                "datasources": [{"name": "x", "connection_url": url}],
                "repositories": [{"name": "y", "repo_url": url}],
            }
        )
        assert safe == {
            "datasources": [{"name": "x"}],
            "repositories": [{"name": "y"}],
        }

    @pytest.mark.parametrize("metadata", [None, {}])
    def test_empty_metadata(self, metadata):
        assert checkpoint_safe_metadata(metadata) == {}

    def test_unexpected_shapes_fail_closed(self):
        metadata = {"datasources": "not-a-list", "repositories": [None, "x"]}
        assert checkpoint_safe_metadata(metadata) == {
            "datasources": [],
            "repositories": [],
        }

    def test_initial_state_holds_safe_metadata(self):
        state = create_initial_state(
            job_id="j1", workspace_path="/ws", metadata=_metadata()
        )

        _assert_no_secret(json.dumps(state["metadata"]))
        assert state["metadata"]["datasources"][0]["name"] == "c0-token-repo"


def _tiny_graph(checkpointer, observe=None):
    """A real StateGraph on the agent's state schema, one node, then END."""

    async def node(state):
        if observe is not None:
            observe(state)
        return {"iteration": state["iteration"] + 1}

    graph = StateGraph(UniversalAgentState)
    graph.add_node("work", node)
    graph.set_entry_point("work")
    graph.add_edge("work", END)
    return graph.compile(checkpointer=checkpointer)


def _checkpoint_files(base: Path) -> bytes:
    return b"".join(path.read_bytes() for path in sorted(base.rglob("job_*.db*")))


@pytest.mark.asyncio
async def test_sqlite_checkpoint_file_holds_no_credential(tmp_path, monkeypatch):
    """The real pinned-pod SQLite saver, read back as raw file bytes."""
    import aiosqlite
    from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver

    path = tmp_path / "job_c0.db"
    async with aiosqlite.connect(path) as conn:
        saver = AsyncSqliteSaver(conn)
        graph = _tiny_graph(saver)
        await graph.ainvoke(
            create_initial_state("c0", str(tmp_path), metadata=_metadata()),
            config={"configurable": {"thread_id": "c0"}},
        )
        state = await graph.aget_state({"configurable": {"thread_id": "c0"}})
        assert state.values["iteration"] == 1

    raw = _checkpoint_files(tmp_path)
    # Positive control: the same bytes do carry the kept connector name.
    assert b"c0-token-repo" in raw
    _assert_no_secret(raw)


# ---------------------------------------------------------------------------
# Every job entry path into graph state, driven through process_job
# ---------------------------------------------------------------------------


def _agent(tmp_path: Path) -> UniversalAgent:
    agent = UniversalAgent.__new__(UniversalAgent)
    agent._initialized = True
    agent._base_config = AgentConfig(agent_id="c0-test", display_name="c0-test")
    agent._base_config.memory.required = False
    agent._auxiliary_llm = None
    agent._citation_verify_aux = None
    agent._llm_with_tools = Mock()
    agent._tools = []
    agent._todo_manager = Mock()
    agent._tool_context = None
    agent._orchestrator_client = None
    agent.postgres_conn = None
    agent._jobs_processed = 0
    agent._workspace_manager = SimpleNamespace(
        path=tmp_path / "ws",
        backend=SimpleNamespace(supports_shell=True),
        exists=lambda _path: False,
    )

    async def setup_workspace(_job_id, metadata, resume=False):
        # The real method returns a shallow copy whose datasources list is
        # the very list the runtime keeps in self._job_metadata.
        return dict(metadata)

    agent._setup_job_workspace = setup_workspace
    agent._remove_legacy_manifest_status = Mock()
    agent._commit_workspace_seed = Mock()
    agent._recover_subagent_orphans = AsyncMock()
    agent._quiesce_subagent_runtime = AsyncMock()
    agent._settle_subagent_runtime = AsyncMock()
    agent._note_resume_without_checkpoint = AsyncMock()
    agent._cleanup_shell_manager = Mock()
    agent._close_datasource_connections = Mock()
    return agent


class TestJobEntryPaths:
    """Fresh start, both resume fallbacks and feedback resume all land in a
    real SQLite checkpoint without a credential, while the runtime keeps its
    credentials in process memory throughout."""

    @pytest.fixture
    def run(self, tmp_path, monkeypatch):
        monkeypatch.setenv("WORKSPACE_PATH", str(tmp_path))
        seen: dict = {}

        async def drive(job_id=None, **kwargs):
            agent = _agent(tmp_path)

            async def setup_tools():
                # Runtime consumers (env install, clone, MCP, credential
                # files) read the dispatch payload in process memory.
                seen["setup"] = copy.deepcopy(agent._job_metadata["datasources"])

            agent._setup_job_tools = setup_tools

            def observe(state):
                seen["state_metadata"] = copy.deepcopy(state["metadata"])
                seen["live"] = copy.deepcopy(agent._job_metadata["datasources"])

            def build_graph(**graph_kwargs):
                return _tiny_graph(graph_kwargs["checkpointer"], observe)

            job_id = job_id or str(uuid4())
            with (
                patch("agent.agent.checkpointer_backend", return_value="sqlite"),
                patch("agent.agent.build_phase_alternation_graph", build_graph),
                patch(
                    "agent.agent.PhaseSnapshotManager",
                    return_value=SimpleNamespace(get_latest_snapshot=lambda: None),
                ),
            ):
                result = await agent.process_job(job_id, _metadata(), **kwargs)
            return agent, result, seen

        return drive

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "kwargs",
        [
            pytest.param({}, id="fresh"),
            pytest.param(
                {"resume": True, "previous_status": "paused"},
                id="graceful-resume-without-checkpoint",
            ),
            pytest.param(
                {"resume": True, "previous_status": "processing"},
                id="crash-resume-without-snapshot",
            ),
            pytest.param(
                {
                    "resume": True,
                    "previous_status": "pending_review",
                    "feedback": "use the other branch",
                },
                id="feedback-resume",
            ),
        ],
    )
    async def test_checkpoint_has_no_credential_and_runtime_keeps_them(
        self, run, tmp_path, kwargs
    ):
        agent, result, seen = await run(**kwargs)

        assert result["iteration"] == 1
        _assert_no_secret(json.dumps(seen["state_metadata"]))
        assert seen["state_metadata"]["datasources"][0]["name"] == "c0-token-repo"
        for payload in (seen["setup"], seen["live"]):
            assert payload[0]["credentials"]["token"] == TOKEN
            assert payload[1]["credentials"]["ssh_key"] == SSH_KEY
            assert payload[2]["credentials"]["env_vars"] == {
                "C0_GATE_SECRET": ENV_SECRET
            }

        raw = _checkpoint_files(tmp_path / "checkpoints")
        assert b"c0-token-repo" in raw
        _assert_no_secret(raw)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("feedback", [None, "use the other branch"])
    async def test_resume_from_an_existing_checkpoint_stays_clean(
        self, run, tmp_path, feedback
    ):
        """A resumed job gets fresh credentials from its new dispatch, and
        neither the checkpoint resume nor the feedback injection writes them
        into the checkpoint it continues."""
        job_id = str(uuid4())
        await run(job_id=job_id)

        agent, result, seen = await run(
            job_id=job_id,
            resume=True,
            previous_status="paused",
            feedback=feedback,
        )

        if feedback:
            assert result["iteration"] == 2
            assert seen["live"][0]["credentials"]["token"] == TOKEN
        assert seen["setup"][0]["credentials"]["token"] == TOKEN
        assert agent._job_metadata["datasources"][0]["credentials"]["token"] == TOKEN
        raw = _checkpoint_files(tmp_path / "checkpoints")
        assert b"c0-token-repo" in raw
        _assert_no_secret(raw)


@pytest.mark.asyncio
async def test_stateless_claim_without_checkpoint_starts_from_safe_state(
    tmp_path, monkeypatch
):
    """A stateless claim with no Postgres checkpoint builds its first state
    from the claim bundle's metadata; the armed input carries no secret."""
    monkeypatch.setenv("WORKSPACE_PATH", str(tmp_path))
    agent = _agent(tmp_path)
    agent._setup_job_tools = AsyncMock()
    saver = InMemorySaver()

    async def make_checkpointer(_job_id):
        agent._checkpointer = saver

    agent._make_checkpointer = make_checkpointer
    agent._retain_compiled_worker_checkpointer = Mock()
    terminal = {"should_stop": True, "freeze_data": {"freeze_type": "c0"}}
    agent._arm_worker_batch = AsyncMock(return_value=terminal)

    with (
        patch(
            "agent.agent.build_phase_alternation_graph",
            lambda **kw: _tiny_graph(kw["checkpointer"]),
        ),
        patch("agent.agent.PhaseSnapshotManager", return_value=None),
    ):
        result = await agent.process_job(
            str(uuid4()),
            _metadata(),
            worker_lease_token=3,
            worker_batch_target_wall_seconds=30,
            defer_cleanup=True,
        )

    assert result == terminal
    graph_input = agent._arm_worker_batch.await_args.kwargs["graph_input"]
    _assert_no_secret(json.dumps(graph_input["metadata"]))
    assert graph_input["metadata"]["datasources"][2]["name"] == "c0-env"
    assert agent._job_metadata["datasources"][0]["credentials"]["token"] == TOKEN
    agent._restore_worker_environment()
