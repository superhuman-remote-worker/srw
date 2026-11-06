"""Tests for the neo4j import guard — the orchestrator-image import heisenbug.

The orchestrator image ships WITHOUT the ``neo4j`` package (graph features are
agent-side). ``src/tools/__init__`` eagerly builds the tool registry, which pulls
``knowledge_tools`` → ``knowledge_graph`` → ``src.database.neo4j_db`` →
``from neo4j import GraphDatabase`` — so the FIRST import of anything under
``src.tools`` (e.g. the PR2 chunker, and through it ``services.kb_reindex``)
raised ModuleNotFoundError on the orchestrator. Worse, the failure was
order-dependent: the failed attempt left ``src.tools.knowledge`` cached in
``sys.modules``, so every RETRY succeeded — the KB sweeper died silently at boot
while the post-merge trigger worked after the first failed merge "polluted" the
cache (live-diagnosed on dev 2026-07-05).

The guard defers the failure to Neo4jDB *construction* so the module chain
imports cleanly without neo4j. These tests block ``neo4j`` in a fresh subprocess
(sys.modules[name] = None makes ``import name`` raise ImportError) — which
reproduces the orchestrator image's dependency set even on a dev box that has
neo4j installed.
"""

import os
import subprocess
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

_BLOCK_NEO4J = (
    "import sys; sys.modules['neo4j'] = None; sys.modules['neo4j.exceptions'] = None; "
)


def _run(code: str) -> subprocess.CompletedProcess:
    env = dict(os.environ)
    env.pop("PYTHONPATH", None)
    env["PYTHONSAFEPATH"] = "1"
    return subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
        env=env,
        timeout=120,
    )


class TestImportChainWithoutNeo4j:
    def test_chunker_imports_without_neo4j(self):
        """The PR2 chunker (under src.tools) must import on a neo4j-less image."""
        proc = _run(
            _BLOCK_NEO4J + "import shared.runtime.knowledge.chunker; print('OK')"
        )
        assert proc.returncode == 0, proc.stderr
        assert "OK" in proc.stdout

    def test_tools_package_imports_without_neo4j(self):
        """The eager registry chain in src/tools/__init__ must survive."""
        proc = _run(_BLOCK_NEO4J + "import agent.tools; print('OK')")
        assert proc.returncode == 0, proc.stderr
        assert "OK" in proc.stdout

    def test_kb_reindex_imports_without_neo4j(self):
        """services.kb_reindex (the sweeper's import) must be deterministic."""
        proc = _run(
            _BLOCK_NEO4J
            + "from orchestrator.services.kb_reindex import reindex_kb; print('OK')",
        )
        assert proc.returncode == 0, proc.stderr
        assert "OK" in proc.stdout

    def test_import_is_order_independent(self):
        """Regression for the heisenbug: first-import and retry must agree."""
        proc = _run(
            _BLOCK_NEO4J
            + (
                """first = None
try:
    import shared.runtime.knowledge.chunker
    first = 'ok'
except Exception as e:
    first = type(e).__name__
import shared.runtime.knowledge.chunker
print('first=' + first + ' retry=ok')
"""
            )
        )
        assert proc.returncode == 0, proc.stderr
        assert "first=ok retry=ok" in proc.stdout


class TestNeo4jDbConstructionGuard:
    def test_construction_raises_without_driver(self, monkeypatch):
        """With the package absent the failure moves to Neo4jDB(...) — loudly."""
        import shared.runtime.database.neo4j_db as neo4j_db

        monkeypatch.setattr(neo4j_db, "GraphDatabase", None)
        with pytest.raises(RuntimeError, match="neo4j"):
            neo4j_db.Neo4jDB(uri="bolt://x:7687", username="u", password="p")


class TestWorkerKnowledgeSetupWithoutNeo4j:
    def test_external_only_scope_initializes_store_without_graph(self):
        from agent.agent import UniversalAgent

        agent = UniversalAgent.__new__(UniversalAgent)
        agent.vector_conn = MagicMock(name="vector_conn")
        agent._current_job_id = "00000000-0000-0000-0000-000000000001"
        agent.config = SimpleNamespace(agent_id="test-agent")
        agent._kb_degraded = False
        agent._knowledge_graph = None
        context = SimpleNamespace(
            knowledge_graph=None,
            knowledge_store=None,
            project_id=None,
            knowledge_bindings=[MagicMock()],
            kb_ids=["00000000-0000-0000-0000-000000000002"],
        )
        knowledge_store = MagicMock(name="knowledge_store")

        with (
            patch("shared.runtime.services.embedding_service.get_kb_embedding_service"),
            patch(
                "shared.runtime.services.knowledge_store.KnowledgeStore",
                return_value=knowledge_store,
            ),
            patch(
                "shared.runtime.services.knowledge_graph.KnowledgeGraphDB"
            ) as graph_cls,
        ):
            agent._setup_job_knowledge(context, None)

        assert context.knowledge_store is knowledge_store
        graph_cls.assert_not_called()

    def test_vector_store_is_available_when_graph_connection_fails(self):
        """Neo4j failure disables graph features, not pgvector KB tools."""
        from agent.agent import UniversalAgent

        agent = UniversalAgent.__new__(UniversalAgent)
        agent.vector_conn = MagicMock(name="vector_conn")
        agent._current_job_id = "00000000-0000-0000-0000-000000000001"
        agent.config = SimpleNamespace(agent_id="test-agent")
        agent._kb_degraded = False
        agent._knowledge_graph = None

        context = SimpleNamespace(
            knowledge_graph=None,
            knowledge_store=None,
            project_id=None,
        )
        embedding_service = MagicMock(name="embedding_service")
        knowledge_store = MagicMock(name="knowledge_store")
        knowledge_graph = MagicMock(name="knowledge_graph")
        knowledge_graph.connect.side_effect = RuntimeError("neo4j unavailable")

        with (
            patch.dict(os.environ, {"NEO4J_URL": "bolt://srw-neo4j:7688"}),
            patch(
                "shared.runtime.services.embedding_service.get_kb_embedding_service",
                return_value=embedding_service,
            ),
            patch(
                "shared.runtime.services.knowledge_store.KnowledgeStore",
                return_value=knowledge_store,
            ) as store_cls,
            patch(
                "shared.runtime.services.knowledge_graph.KnowledgeGraphDB",
                return_value=knowledge_graph,
            ),
        ):
            agent._setup_job_knowledge(context, "project-1")

        store_cls.assert_called_once_with(
            db=agent.vector_conn,
            embedding_service=embedding_service,
        )
        assert context.knowledge_store is knowledge_store
        assert context.knowledge_graph is None
        assert context.project_id == "project-1"
        assert agent._knowledge_graph is None
        assert agent._kb_degraded is False

    def test_unconfigured_neo4j_skips_the_graph_tier_without_dialling(self, caplog):
        """No NEO4J_URL (Neo4j is off by default) means no localhost:7687 dial."""
        import logging

        from agent.agent import UniversalAgent

        agent = UniversalAgent.__new__(UniversalAgent)
        agent.vector_conn = MagicMock(name="vector_conn")
        agent._current_job_id = "00000000-0000-0000-0000-000000000001"
        agent.config = SimpleNamespace(agent_id="test-agent")
        agent._kb_degraded = False
        agent._knowledge_graph = None
        context = SimpleNamespace(
            knowledge_graph=None,
            knowledge_store=None,
            project_id=None,
        )
        knowledge_store = MagicMock(name="knowledge_store")
        env = {k: v for k, v in os.environ.items() if k != "NEO4J_URL"}

        with (
            patch.dict(os.environ, env, clear=True),
            patch("shared.runtime.services.embedding_service.get_kb_embedding_service"),
            patch(
                "shared.runtime.services.knowledge_store.KnowledgeStore",
                return_value=knowledge_store,
            ),
            patch(
                "shared.runtime.services.knowledge_graph.KnowledgeGraphDB"
            ) as graph_cls,
            caplog.at_level(logging.INFO),
        ):
            agent._setup_job_knowledge(context, "project-1")

        graph_cls.assert_not_called()
        assert context.knowledge_store is knowledge_store
        assert context.knowledge_graph is None
        assert agent._kb_degraded is False
        assert "NEO4J_URL is not set" in caplog.text
        assert not [r for r in caplog.records if r.levelno >= logging.WARNING]

    def test_missing_vector_connection_marks_store_degraded(self):
        """A genuinely unavailable retrieval store still fails honestly."""
        from agent.agent import UniversalAgent

        agent = UniversalAgent.__new__(UniversalAgent)
        agent.vector_conn = None
        agent._current_job_id = "00000000-0000-0000-0000-000000000001"
        agent.config = SimpleNamespace(agent_id="test-agent")
        agent._kb_degraded = False
        agent._knowledge_graph = None
        context = SimpleNamespace(
            knowledge_graph=None,
            knowledge_store=None,
            project_id=None,
        )
        knowledge_graph = MagicMock()
        knowledge_graph.connect.return_value = False

        with (
            patch("agent.core.archiver.audit_unavailable") as audit_unavailable,
            patch(
                "shared.runtime.services.knowledge_graph.KnowledgeGraphDB",
                return_value=knowledge_graph,
            ),
        ):
            agent._setup_job_knowledge(context, "project-1")

        assert context.knowledge_store is None
        assert agent._kb_degraded is True
        audit_unavailable.assert_called_once()
