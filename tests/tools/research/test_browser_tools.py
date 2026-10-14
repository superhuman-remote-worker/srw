"""Tests for browser tools and network utilities."""

import re
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from agent.tools.research.utils.network import ProxyConfig, ProxyType
from agent.tools.research.utils.paper_types import PaperSource


# ── ProxyConfig tests ──────────────────────────────────────────────


class TestProxyType:
    """Tests for ProxyType enum."""

    def test_enum_values(self):
        assert ProxyType.NONE.value == "none"
        assert ProxyType.HTTP.value == "http"
        assert ProxyType.SOCKS5.value == "socks5"


class TestProxyConfig:
    """Tests for ProxyConfig dataclass."""

    def test_default_is_unconfigured(self):
        config = ProxyConfig()
        assert config.type == ProxyType.NONE
        assert config.is_configured is False
        assert config.url is None

    def test_configured_socks5(self):
        config = ProxyConfig(type=ProxyType.SOCKS5, host="localhost", port=1080)
        assert config.is_configured is True
        assert config.url == "socks5://localhost:1080"

    def test_configured_http(self):
        config = ProxyConfig(type=ProxyType.HTTP, host="proxy.uni.edu", port=8080)
        assert config.url == "http://proxy.uni.edu:8080"

    def test_url_with_auth(self):
        config = ProxyConfig(
            type=ProxyType.SOCKS5,
            host="localhost",
            port=1080,
            username="user",
            password="pass",
        )
        assert config.url == "socks5://user:pass@localhost:1080"

    def test_not_configured_without_host(self):
        config = ProxyConfig(type=ProxyType.SOCKS5, port=1080)
        assert config.is_configured is False

    def test_not_configured_without_port(self):
        config = ProxyConfig(type=ProxyType.SOCKS5, host="localhost")
        assert config.is_configured is False


class TestProxyConfigFromEnv:
    """Tests for ProxyConfig.from_env class method."""

    def test_from_env_none(self):
        with patch.dict("os.environ", {}, clear=True):
            config = ProxyConfig.from_env()
            assert config.type == ProxyType.NONE

    def test_from_env_socks5(self):
        env = {
            "RESEARCH_PROXY_TYPE": "socks5",
            "RESEARCH_PROXY_HOST": "localhost",
            "RESEARCH_PROXY_PORT": "1080",
        }
        with patch.dict("os.environ", env, clear=True):
            config = ProxyConfig.from_env()
            assert config.type == ProxyType.SOCKS5
            assert config.host == "localhost"
            assert config.port == 1080
            assert config.is_configured is True

    def test_from_env_with_auth(self):
        env = {
            "RESEARCH_PROXY_TYPE": "http",
            "RESEARCH_PROXY_HOST": "proxy.example.com",
            "RESEARCH_PROXY_PORT": "8080",
            "RESEARCH_PROXY_USER": "admin",
            "RESEARCH_PROXY_PASS": "secret",
        }
        with patch.dict("os.environ", env, clear=True):
            config = ProxyConfig.from_env()
            assert config.username == "admin"
            assert config.password == "secret"

    def test_from_env_unknown_type_falls_back(self):
        env = {"RESEARCH_PROXY_TYPE": "invalid_type"}
        with patch.dict("os.environ", env, clear=True):
            config = ProxyConfig.from_env()
            assert config.type == ProxyType.NONE


class TestProxyConfigFromConfig:
    """Tests for ProxyConfig.from_config class method."""

    def test_from_config_enabled(self):
        config_dict = {
            "enabled": True,
            "type": "socks5",
            "host": "localhost",
            "port": 1080,
        }
        config = ProxyConfig.from_config(config_dict)
        assert config.type == ProxyType.SOCKS5
        assert config.host == "localhost"
        assert config.port == 1080

    def test_from_config_disabled_falls_back_to_env(self):
        env = {
            "RESEARCH_PROXY_TYPE": "http",
            "RESEARCH_PROXY_HOST": "env-proxy",
            "RESEARCH_PROXY_PORT": "3128",
        }
        with patch.dict("os.environ", env, clear=True):
            config = ProxyConfig.from_config({"enabled": False})
            assert config.type == ProxyType.HTTP
            assert config.host == "env-proxy"

    def test_from_config_empty_falls_back_to_env(self):
        with patch.dict("os.environ", {}, clear=True):
            config = ProxyConfig.from_config({})
            assert config.type == ProxyType.NONE

    def test_from_config_none_falls_back_to_env(self):
        with patch.dict("os.environ", {}, clear=True):
            config = ProxyConfig.from_config(None)
            assert config.type == ProxyType.NONE

    def test_from_config_auth_from_env(self):
        config_dict = {
            "enabled": True,
            "type": "socks5",
            "host": "localhost",
            "port": 1080,
        }
        env = {"RESEARCH_PROXY_USER": "user", "RESEARCH_PROXY_PASS": "pass"}
        with patch.dict("os.environ", env, clear=True):
            config = ProxyConfig.from_config(config_dict)
            assert config.username == "user"
            assert config.password == "pass"


# ── No-local-browser regression guard ──────────────────────────────


class TestNoLocalBrowserPath:
    """The agent runtime must not contain an in-pod browser execution path.

    The local browser_use fallback was removed — the workspace-side
    browser-exec daemon is the only browser (see
    knowledge-base/knowledge/issues/remove_local_browser_fallback.md). A reintroduced import
    would put a JS-executing engine back inside the credential-holding
    agent pod.
    """

    def test_browser_use_not_imported_under_src(self):
        src_root = Path(__file__).resolve().parents[3] / "src" / "agent"
        pattern = re.compile(r"^\s*(?:from|import)\s+browser_use", re.MULTILINE)
        offenders = sorted(
            str(path.relative_to(src_root))
            for path in src_root.rglob("*.py")
            if pattern.search(path.read_text(encoding="utf-8"))
        )
        assert offenders == [], (
            f"browser_use must not be imported in the agent runtime: {offenders}"
        )


# ── Workflow tools tests ───────────────────────────────────────────


class TestWorkflowToolsMetadata:
    """Tests for workflow tools metadata."""

    def test_metadata_has_research_topic(self):
        from agent.tools.research.workflow import WORKFLOW_TOOLS_METADATA

        assert "research_topic" in WORKFLOW_TOOLS_METADATA

    def test_metadata_category(self):
        from agent.tools.research.workflow import WORKFLOW_TOOLS_METADATA

        assert WORKFLOW_TOOLS_METADATA["research_topic"]["category"] == "research"


class TestDeduplicatePapers:
    """Tests for _deduplicate_papers function."""

    def test_deduplicates_by_doi(self, sample_paper, sample_paper_s2):
        from agent.tools.research.workflow import _deduplicate_papers

        # Give them the same DOI
        sample_paper_s2.doi = sample_paper.doi

        result = _deduplicate_papers([sample_paper], [sample_paper_s2])

        # S2 is processed first, so it should be kept
        assert len(result) == 1
        assert result[0].source == PaperSource.SEMANTIC_SCHOLAR

    def test_deduplicates_by_arxiv_id(self, sample_paper, sample_paper_s2):
        from agent.tools.research.workflow import _deduplicate_papers

        # Different DOIs but same arXiv ID
        sample_paper.doi = None
        sample_paper_s2.doi = None
        sample_paper_s2.arxiv_id = sample_paper.arxiv_id

        result = _deduplicate_papers([sample_paper], [sample_paper_s2])
        assert len(result) == 1

    def test_keeps_unique_papers(self, sample_paper, sample_paper_s2):
        from agent.tools.research.workflow import _deduplicate_papers

        # Different identifiers
        sample_paper.doi = "10.1/a"
        sample_paper.arxiv_id = "1111.11111"
        sample_paper_s2.doi = "10.1/b"
        sample_paper_s2.arxiv_id = "2222.22222"

        result = _deduplicate_papers([sample_paper], [sample_paper_s2])
        assert len(result) == 2

    def test_empty_inputs(self):
        from agent.tools.research.workflow import _deduplicate_papers

        result = _deduplicate_papers([], [])
        assert result == []

    def test_prefers_semantic_scholar(self, sample_paper, sample_paper_s2):
        from agent.tools.research.workflow import _deduplicate_papers

        # Same DOI - S2 should win since it's processed first
        sample_paper_s2.doi = sample_paper.doi
        sample_paper_s2.citation_count = 5000

        result = _deduplicate_papers([sample_paper], [sample_paper_s2])
        assert len(result) == 1
        assert result[0].citation_count == 5000


class TestResearchTopic:
    """Tests for research_topic tool."""

    @pytest.mark.asyncio
    async def test_research_topic_combines_sources(
        self, mock_tool_context, sample_paper, sample_paper_s2
    ):
        from agent.tools.research.workflow import create_workflow_tools

        tools = create_workflow_tools(mock_tool_context)
        research_topic = tools[0]

        # Different papers from each source
        sample_paper.doi = "10.1/arxiv"
        sample_paper.arxiv_id = "1111.11111"
        sample_paper_s2.doi = "10.1/s2"
        sample_paper_s2.arxiv_id = "2222.22222"

        with (
            patch(
                "agent.tools.research.workflow._search_arxiv_raw",
                new_callable=AsyncMock,
                return_value=[sample_paper],
            ),
            patch(
                "agent.tools.research.workflow._search_semantic_scholar_raw",
                new_callable=AsyncMock,
                return_value=[sample_paper_s2],
            ),
        ):
            result = await research_topic.ainvoke(
                {"topic": "transformers", "download_available": False}
            )

        assert "Research Report" in result
        assert "transformers" in result
        assert "Unique papers after deduplication: 2" in result

    @pytest.mark.asyncio
    async def test_research_topic_handles_search_failure(
        self, mock_tool_context, sample_paper
    ):
        from agent.tools.research.workflow import create_workflow_tools

        tools = create_workflow_tools(mock_tool_context)
        research_topic = tools[0]

        with (
            patch(
                "agent.tools.research.workflow._search_arxiv_raw",
                new_callable=AsyncMock,
                return_value=[sample_paper],
            ),
            patch(
                "agent.tools.research.workflow._search_semantic_scholar_raw",
                new_callable=AsyncMock,
                side_effect=Exception("API error"),
            ),
        ):
            result = await research_topic.ainvoke(
                {"topic": "test", "download_available": False}
            )

        # Should still return results from arXiv
        assert "Research Report" in result
        assert "Provider warnings:" in result
        assert "Semantic Scholar search failed: API error" in result

    @pytest.mark.asyncio
    async def test_research_topic_no_results(self, mock_tool_context):
        from agent.tools.research.workflow import create_workflow_tools

        tools = create_workflow_tools(mock_tool_context)
        research_topic = tools[0]

        with (
            patch(
                "agent.tools.research.workflow._search_arxiv_raw",
                new_callable=AsyncMock,
                return_value=[],
            ),
            patch(
                "agent.tools.research.workflow._search_semantic_scholar_raw",
                new_callable=AsyncMock,
                return_value=[],
            ),
        ):
            result = await research_topic.ainvoke({"topic": "xyznonexistent"})

        assert "No results found" in result
        assert "Provider failures:" not in result

    @pytest.mark.asyncio
    async def test_research_topic_both_fail(self, mock_tool_context):
        from agent.tools.research.workflow import create_workflow_tools

        tools = create_workflow_tools(mock_tool_context)
        research_topic = tools[0]

        with (
            patch(
                "agent.tools.research.workflow._search_arxiv_raw",
                new_callable=AsyncMock,
                side_effect=Exception("arXiv down"),
            ),
            patch(
                "agent.tools.research.workflow._search_semantic_scholar_raw",
                new_callable=AsyncMock,
                side_effect=Exception("S2 down"),
            ),
        ):
            result = await research_topic.ainvoke({"topic": "test"})

        assert "No results found" in result
        assert "Provider failures:" in result
        assert "arXiv search failed: arXiv down" in result
        assert "Semantic Scholar search failed: S2 down" in result

    @pytest.mark.asyncio
    async def test_research_topic_caps_num_papers(self, mock_tool_context):
        from agent.tools.research.workflow import create_workflow_tools

        tools = create_workflow_tools(mock_tool_context)
        research_topic = tools[0]

        with (
            patch(
                "agent.tools.research.workflow._search_arxiv_raw",
                new_callable=AsyncMock,
                return_value=[],
            ),
            patch(
                "agent.tools.research.workflow._search_semantic_scholar_raw",
                new_callable=AsyncMock,
                return_value=[],
            ),
        ):
            result = await research_topic.ainvoke({"topic": "test", "num_papers": 100})

        # Should cap to 30 (max) - no error even with large value
        assert isinstance(result, str)


class TestFormatResearchReport:
    """Tests for _format_research_report function."""

    def test_format_basic_report(self, sample_paper):
        from agent.tools.research.workflow import _format_research_report

        report = _format_research_report(
            topic="test topic",
            papers=[sample_paper],
            download_results=[],
            arxiv_count=1,
            s2_count=0,
            include_abstracts=True,
        )

        assert "Research Report: test topic" in report
        assert "arXiv (1 results)" in report
        assert "Semantic Scholar (0 results)" in report
        assert "Unique papers after deduplication: 1" in report
        assert sample_paper.title in report

    def test_format_with_downloads(self, sample_paper):
        from agent.tools.research.workflow import _format_research_report

        report = _format_research_report(
            topic="test",
            papers=[sample_paper],
            download_results=["  Downloaded: paper.pdf"],
            arxiv_count=1,
            s2_count=0,
            include_abstracts=False,
        )

        assert "Downloads:" in report
        assert "Downloaded: paper.pdf" in report

    def test_format_truncates_abstracts(self, sample_paper):
        from agent.tools.research.workflow import _format_research_report

        sample_paper.abstract = "x" * 500

        report = _format_research_report(
            topic="test",
            papers=[sample_paper],
            download_results=[],
            arxiv_count=1,
            s2_count=0,
            include_abstracts=True,
        )

        assert "..." in report

    def test_format_without_abstracts(self, sample_paper):
        from agent.tools.research.workflow import _format_research_report

        report = _format_research_report(
            topic="test",
            papers=[sample_paper],
            download_results=[],
            arxiv_count=1,
            s2_count=0,
            include_abstracts=False,
        )

        assert "Abstract:" not in report


# ── Research __init__ integration test ─────────────────────────────


class TestResearchToolsRegistry:
    """Tests for the research tools registry."""

    def test_get_research_metadata_includes_all_modules(self):
        from agent.tools.research import get_research_metadata

        metadata = get_research_metadata()

        # Web tools
        assert "web_search" in metadata
        assert "extract_webpage" in metadata
        assert "crawl_website" in metadata
        assert "map_website" in metadata

        # Paper tools
        assert "search_papers" in metadata
        assert "download_paper" in metadata
        assert "get_paper_info" in metadata

        # Autonomous browser tools were deprecated (direct browser_* tools instead)
        assert "browse_website" not in metadata
        assert "download_from_website" not in metadata

        # Workflow tools
        assert "research_topic" in metadata

    def test_create_research_tools_returns_all(self, mock_tool_context):
        from agent.tools.research import create_research_tools

        tools = create_research_tools(mock_tool_context)
        names = {t.name for t in tools}

        assert "web_search" in names
        assert "extract_webpage" in names
        assert "crawl_website" in names
        assert "map_website" in names
        assert "search_papers" in names
        assert "download_paper" in names
        assert "get_paper_info" in names
        assert "research_topic" in names
        # Autonomous browser tools were deprecated
        assert "browse_website" not in names
        assert "download_from_website" not in names


# ── Browser-exec dispatch tests (workspace executor) ───────────────


class TestToolContextBrowserExec:
    """Tests for ToolContext.browser_exec stdout-JSON parsing."""

    @pytest.mark.asyncio
    async def test_parses_json_stdout(self):
        from agent.tools.context import ToolContext

        fake = MagicMock()
        backend = MagicMock(spec=["exec_command"])
        backend.exec_command = MagicMock(
            return_value='{"dom": "x", "url": "https://e.com", "title": "E"}'
        )
        fake.workspace_manager.backend = backend
        result = await ToolContext.browser_exec(fake, "navigate", url="https://e.com")
        assert result["url"] == "https://e.com"
        cmd = backend.exec_command.call_args[0][0]
        assert cmd.startswith("browser-exec navigate --json")

    @pytest.mark.asyncio
    async def test_stateless_backend_uses_claim_resource_fence(self):
        from agent.tools.context import ToolContext

        fake = MagicMock()
        backend = MagicMock(spec=["exec_command", "exec_claim_resource"])
        backend.exec_claim_resource = MagicMock(return_value='{"url": "u"}')
        fake.workspace_manager.backend = backend

        result = await ToolContext.browser_exec(fake, "snapshot")

        assert result == {"url": "u"}
        backend.exec_command.assert_not_called()
        command, timeout = backend.exec_claim_resource.call_args.args
        assert command.startswith("browser-exec snapshot --json")
        assert timeout == 200
        assert backend.exec_claim_resource.call_args.kwargs == {
            "operation": "browser-exec snapshot"
        }

    @pytest.mark.asyncio
    async def test_claim_fence_failure_never_falls_back_to_plain_exec(self):
        from agent.tools.context import ToolContext

        fake = MagicMock()
        backend = MagicMock(spec=["exec_command", "exec_claim_resource"])
        backend.exec_claim_resource = MagicMock(side_effect=RuntimeError("stale claim"))
        fake.workspace_manager.backend = backend

        result = await ToolContext.browser_exec(fake, "snapshot")

        assert "error" in result
        assert "stale claim" in result["error"]
        backend.exec_command.assert_not_called()

    @pytest.mark.asyncio
    async def test_non_json_becomes_error(self):
        from agent.tools.context import ToolContext

        fake = MagicMock()
        backend = MagicMock(spec=["exec_command"])
        backend.exec_command = MagicMock(return_value="Traceback: boom")
        fake.workspace_manager.backend = backend
        result = await ToolContext.browser_exec(fake, "snapshot")
        assert "error" in result

    @pytest.mark.asyncio
    async def test_empty_output_becomes_error(self):
        from agent.tools.context import ToolContext

        fake = MagicMock()
        backend = MagicMock(spec=["exec_command"])
        backend.exec_command = MagicMock(return_value="")
        fake.workspace_manager.backend = backend
        result = await ToolContext.browser_exec(fake, "snapshot")
        assert "error" in result

    @pytest.mark.asyncio
    async def test_parses_last_stdout_line(self):
        from agent.tools.context import ToolContext

        fake = MagicMock()
        backend = MagicMock(spec=["exec_command"])
        backend.exec_command = MagicMock(
            return_value='noise on stdout\n{"url": "u", "dom": "d"}'
        )
        fake.workspace_manager.backend = backend
        result = await ToolContext.browser_exec(fake, "snapshot")
        assert result["url"] == "u"

    @pytest.mark.asyncio
    async def test_strict_close_requires_physical_shutdown_ack(self):
        from agent.tools.context import ToolContext

        fake = MagicMock()
        fake.has_workspace.return_value = True
        fake.browser_exec = AsyncMock(
            return_value={"ok": True, "shutdown_complete": True}
        )

        await ToolContext.close_browser(fake, strict=True)

        fake.browser_exec.assert_awaited_once_with("shutdown")

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "result",
        [
            {"error": "shutdown failed"},
            {"ok": True},
            {},
            "not-an-object",
        ],
    )
    async def test_strict_close_rejects_unacknowledged_shutdown(self, result):
        from agent.tools.context import ToolContext

        fake = MagicMock()
        fake.has_workspace.return_value = True
        fake.browser_exec = AsyncMock(return_value=result)

        with pytest.raises(RuntimeError, match="shutdown was not acknowledged"):
            await ToolContext.close_browser(fake, strict=True)


class TestBrowserDirectDispatch:
    """Direct browser tools route to browser_exec on a remote workspace."""

    def _ctx(self, mock_remote_tool_context, exec_result):
        ctx = mock_remote_tool_context
        ctx.browser_exec = AsyncMock(return_value=exec_result)
        ctx.should_include_screenshots = MagicMock(return_value=False)
        ctx.get_max_dom_chars = MagicMock(return_value=40000)
        return ctx

    @pytest.mark.asyncio
    async def test_navigate_dispatches_and_wraps_nonce(self, mock_remote_tool_context):
        from agent.tools.research.browser_direct import create_browser_direct_tools

        ctx = self._ctx(
            mock_remote_tool_context,
            {"dom": "[1]<button>Go</button>", "url": "https://e.com", "title": "E"},
        )
        tools = {t.name: t for t in create_browser_direct_tools(ctx)}
        result = await tools["browser_navigate"].ainvoke({"url": "https://example.com"})

        ctx.browser_exec.assert_awaited_once()
        assert ctx.browser_exec.await_args[0][0] == "navigate"
        assert ctx.browser_exec.await_args[1]["url"] == "https://example.com"
        # Result is now a formatted string (see _page_state_to_text) with the
        # nonce-wrapped DOM inline, not a dict.
        assert isinstance(result, str)
        assert "page_content nonce=" in result
        assert "https://e.com" in result

    @pytest.mark.asyncio
    async def test_invalid_url_short_circuits(self, mock_remote_tool_context):
        from agent.tools.research.browser_direct import create_browser_direct_tools

        ctx = self._ctx(mock_remote_tool_context, {})
        tools = {t.name: t for t in create_browser_direct_tools(ctx)}
        result = await tools["browser_navigate"].ainvoke({"url": "file:///etc/passwd"})

        assert isinstance(result, str)
        assert "error" in result.lower()
        ctx.browser_exec.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_click_passes_ref(self, mock_remote_tool_context):
        from agent.tools.research.browser_direct import create_browser_direct_tools

        ctx = self._ctx(
            mock_remote_tool_context, {"dom": "d", "url": "u", "title": "t"}
        )
        tools = {t.name: t for t in create_browser_direct_tools(ctx)}
        await tools["browser_click"].ainvoke({"ref": 42})

        assert ctx.browser_exec.await_args[0][0] == "click"
        assert ctx.browser_exec.await_args[1]["ref"] == 42

    @pytest.mark.asyncio
    async def test_exec_error_returned_unwrapped(self, mock_remote_tool_context):
        from agent.tools.research.browser_direct import create_browser_direct_tools

        ctx = self._ctx(mock_remote_tool_context, {"error": "navigate failed: boom"})
        tools = {t.name: t for t in create_browser_direct_tools(ctx)}
        result = await tools["browser_navigate"].ainvoke({"url": "https://example.com"})

        assert isinstance(result, str)
        assert "navigate failed: boom" in result
        assert "nonce" not in result

    @pytest.mark.asyncio
    async def test_snapshot_surfaces_known_browser_control_holder(
        self, mock_remote_tool_context
    ):
        from agent.tools.research.browser_direct import create_browser_direct_tools

        ctx = self._ctx(
            mock_remote_tool_context,
            {
                "dom": "[1]<button>Continue</button>",
                "url": "https://example.test/form",
                "title": "Form",
                "baton": "user",
            },
        )
        tools = {t.name: t for t in create_browser_direct_tools(ctx)}

        result = await tools["browser_snapshot"].ainvoke({})

        assert "URL: https://example.test/form" in result
        assert "Title: Form" in result
        assert "Browser control: user" in result
        assert result.index("Browser control: user") < result.index(
            "page_content nonce="
        )

    @pytest.mark.asyncio
    async def test_user_driving_refusal_is_actionable_prompt_visible_prose(
        self, mock_remote_tool_context
    ):
        from agent.tools.research.browser_direct import create_browser_direct_tools

        daemon_message = "Interactive browser input is reserved for the user."
        ctx = self._ctx(
            mock_remote_tool_context,
            {
                "error": "user_is_driving",
                "url": "https://example.test/form",
                "message": daemon_message,
                "dom": "must not be interpreted",
            },
        )
        tools = {t.name: t for t in create_browser_direct_tools(ctx)}

        result = await tools["browser_click"].ainvoke({"ref": 1})

        assert "Browser error: user_is_driving" not in result
        assert "https://example.test/form" in result
        assert daemon_message in result
        assert "Read-only browser snapshots still work" in result
        assert "Ask the user to release browser control" in result
        assert "page_content nonce=" not in result
        assert "must not be interpreted" not in result

    @pytest.mark.parametrize("baton", ["observer", "", None, 1])
    @pytest.mark.asyncio
    async def test_snapshot_ignores_unknown_control_holder(
        self, mock_remote_tool_context, baton
    ):
        from agent.tools.research.browser_direct import create_browser_direct_tools

        ctx = self._ctx(
            mock_remote_tool_context,
            {"dom": "page", "url": "https://e.com", "baton": baton},
        )
        tools = {t.name: t for t in create_browser_direct_tools(ctx)}

        result = await tools["browser_snapshot"].ainvoke({})

        assert "Browser control:" not in result

    @pytest.mark.asyncio
    async def test_screenshot_emitted_as_image_tag(self, mock_remote_tool_context):
        """A screenshot is surfaced as an <image_data> tag so the graph-side
        extract_image_tags post-processor lifts it into a real image block
        (and the base64 never reaches the token counter as text)."""
        from agent.tools.research.browser_direct import create_browser_direct_tools
        from agent.services.image_content import extract_image_tags

        ctx = self._ctx(
            mock_remote_tool_context,
            {
                "url": "https://e.com",
                "title": "E",
                "screenshot": "iVBORw0KGgoAAAANSUhEUg==",
            },
        )
        tools = {t.name: t for t in create_browser_direct_tools(ctx)}
        result = await tools["browser_screenshot"].ainvoke({})

        assert isinstance(result, str)
        assert '<image_data mime_type="image/png">' in result
        cleaned, images = extract_image_tags(result)
        assert len(images) == 1
        assert images[0].mime_type == "image/png"
        assert "iVBORw0KGgoAAAANSUhEUg==" not in cleaned
        assert "[image attached: image/png]" in cleaned
