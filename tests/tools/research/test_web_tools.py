"""Tests for web search tools (Tavily Search, Extract, Crawl, Map)."""

import logging
import sys
from types import ModuleType
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from agent.tools.context import ToolContext
from agent.tools.research.search import (
    ProviderAuthError,
    ProviderQuotaError,
    ProviderRateLimitError,
    ProviderRequestError,
    ProviderUnavailableError,
    Result,
    TavilyAdapter,
)
from agent.tools.research.web import (
    MAX_TOTAL_INLINE_CHARS,
    RESEARCH_TOOLS_METADATA,
    _crawl_website,
    _direct_web_search,
    _extract_webpage,
    _map_website,
    _parse_comma_list,
    _truncate_content,
    create_web_tools,
)

TAVILY_CONFIG = {
    "provider": "tavily",
    "api_key": "tvly-test",
    "ops": ["search", "extract", "crawl", "map"],
}


def _configure_tavily(context):
    context.config = {"research": {"search": TAVILY_CONFIG, "fetch": TAVILY_CONFIG}}
    return context


def _remove_tavily_key(context):
    context.config = {
        "research": {
            capability: {**TAVILY_CONFIG, "api_key": None}
            for capability in ("search", "fetch")
        }
    }
    return context


@pytest.fixture
def mock_langchain_tavily():
    """Create a mock langchain_tavily module and inject into sys.modules."""
    mock_mod = ModuleType("langchain_tavily")
    mock_mod.TavilySearch = MagicMock()
    mock_mod.TavilyExtract = MagicMock()
    mock_mod.TavilyCrawl = MagicMock()
    mock_mod.TavilyMap = MagicMock()
    with patch.dict(sys.modules, {"langchain_tavily": mock_mod}):
        yield mock_mod


# ── Helpers ────────────────────────────────────────────────────────


class _TempWorkspaceBackend:
    """Minimal local workspace backend for ToolContext disk writes."""

    host = None

    def __init__(self, root):
        self.root = root

    def mkdir(self, relative_path):
        (self.root / relative_path).mkdir(parents=True, exist_ok=True)


class _TempWorkspaceManager:
    """Minimal WorkspaceManager-shaped object accepted by ToolContext."""

    is_initialized = True
    job_id = "test-job-web"

    def __init__(self, root):
        self.root = root
        self.backend = _TempWorkspaceBackend(root)

    def exists(self, relative_path):
        return (self.root / relative_path).exists()

    def write_file(self, relative_path, content):
        path = self.root / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)


def _make_disk_context(tmp_path, register_side_effect=None):
    """Create a ToolContext that registers fake sources and writes real files."""
    context = _configure_tavily(
        ToolContext(workspace_manager=_TempWorkspaceManager(tmp_path))
    )

    if register_side_effect is None:
        source_ids = {}

        async def default_register_source(url, name=None, content=None):
            del name, content
            source_ids.setdefault(url, len(source_ids) + 1)
            return source_ids[url], None

        register_side_effect = default_register_source

    context.get_or_register_web_source = AsyncMock(side_effect=register_side_effect)
    return context


def _make_no_workspace_context():
    """Create a ToolContext whose web source registration works but saving fails."""
    context = _configure_tavily(ToolContext())

    async def register_source(url, name=None, content=None):
        del url, name, content
        return 1, None

    context.get_or_register_web_source = AsyncMock(side_effect=register_source)
    return context


class TestHelpers:
    """Tests for module-level helper functions."""

    def test_parse_comma_list_single(self):
        assert _parse_comma_list("example.com") == ["example.com"]

    def test_parse_comma_list_multiple(self):
        assert _parse_comma_list("a.com, b.com, c.com") == ["a.com", "b.com", "c.com"]

    def test_parse_comma_list_none(self):
        assert _parse_comma_list(None) is None

    def test_parse_comma_list_empty(self):
        assert _parse_comma_list("") is None

    def test_parse_comma_list_whitespace_only(self):
        assert _parse_comma_list("  ,  , ") is None

    def test_truncate_content_within_limit(self):
        text = "word " * 100
        result = _truncate_content(text.strip(), max_words=200)
        assert "truncated" not in result

    def test_truncate_content_over_limit(self):
        text = "word " * 6000
        result = _truncate_content(text.strip(), max_words=5000)
        assert "truncated from 6000 words" in result
        assert len(result.split()) < 5010  # 5000 words + truncation note


# ── Metadata ───────────────────────────────────────────────────────


class TestWebToolsMetadata:
    """Tests for RESEARCH_TOOLS_METADATA entries."""

    def test_metadata_has_all_tools(self):
        expected = {"web_search", "extract_webpage", "crawl_website", "map_website"}
        assert set(RESEARCH_TOOLS_METADATA.keys()) == expected

    def test_metadata_category_is_research(self):
        for name, meta in RESEARCH_TOOLS_METADATA.items():
            assert meta["category"] == "research", f"{name} has wrong category"

    def test_metadata_phases_tactical(self):
        for name, meta in RESEARCH_TOOLS_METADATA.items():
            assert meta["phases"] == ["tactical"], f"{name} has wrong phases"


# ── Tool creation ──────────────────────────────────────────────────


class TestCreateWebTools:
    """Tests for create_web_tools factory."""

    def test_creates_four_tools(self, mock_tool_context):
        tools = create_web_tools(mock_tool_context)
        assert len(tools) == 4

    def test_tool_names(self, mock_tool_context):
        tools = create_web_tools(mock_tool_context)
        names = {t.name for t in tools}
        assert names == {
            "web_search",
            "extract_webpage",
            "crawl_website",
            "map_website",
        }

    def test_no_provider_constructs_no_tools_but_metadata_stays_complete(
        self, mock_tool_context
    ):
        mock_tool_context.config = {"research": {}}

        assert create_web_tools(mock_tool_context) == []
        assert set(RESEARCH_TOOLS_METADATA) == {
            "web_search",
            "extract_webpage",
            "crawl_website",
            "map_website",
        }

    def test_search_only_provider_constructs_exactly_web_search(
        self, mock_tool_context
    ):
        mock_tool_context.config = {
            "research": {
                "search": {
                    "provider": "searxng",
                    "base_url": "https://search.internal",
                    "api_key": None,
                    "ops": ["search"],
                }
            }
        }

        assert [tool.name for tool in create_web_tools(mock_tool_context)] == [
            "web_search"
        ]

    def test_crawl4ai_constructs_only_its_fetch_tools(self, mock_tool_context):
        mock_tool_context.config = {
            "research": {
                "fetch": {
                    "provider": "crawl4ai",
                    "base_url": "https://crawl4ai.internal",
                    "api_key": "crawl4ai-test",
                    "ops": ["extract", "crawl"],
                }
            }
        }

        assert [tool.name for tool in create_web_tools(mock_tool_context)] == [
            "extract_webpage",
            "crawl_website",
        ]

    def test_crawl4ai_row_declaring_map_constructs_map_website(self, mock_tool_context):
        mock_tool_context.config = {
            "research": {
                "fetch": {
                    "provider": "crawl4ai",
                    "base_url": "https://crawl4ai.internal",
                    "api_key": "crawl4ai-test",
                    "ops": ["extract", "crawl", "map"],
                }
            }
        }

        assert [tool.name for tool in create_web_tools(mock_tool_context)] == [
            "extract_webpage",
            "crawl_website",
            "map_website",
        ]


# ── web_search ─────────────────────────────────────────────────────


class TestWebSearch:
    """Tests for the enhanced web_search tool."""

    def _make_search_response(self, n=2, include_raw=False):
        results = []
        for i in range(n):
            r = {
                "url": f"https://example{i}.com",
                "title": f"Result {i}",
                "content": f"Short snippet for result {i}",
            }
            if include_raw:
                r["raw_content"] = f"Full content for result {i} " + ("word " * 100)
            results.append(r)
        return {"results": results}

    def test_basic_search(self, mock_tool_context, mock_langchain_tavily):
        mock_instance = MagicMock()
        mock_instance.invoke.return_value = self._make_search_response()
        mock_langchain_tavily.TavilySearch.return_value = mock_instance

        tools = create_web_tools(mock_tool_context)
        ws = next(t for t in tools if t.name == "web_search")
        result = ws.invoke({"query": "test query"})

        assert "Web Search Results for: test query" in result
        assert "example0.com" in result
        assert "example1.com" in result

    def test_missing_api_key(self, mock_tool_context):
        _remove_tavily_key(mock_tool_context)
        tools = create_web_tools(mock_tool_context)
        ws = next(t for t in tools if t.name == "web_search")
        result = ws.invoke({"query": "test"})
        assert "Tavily API key is not configured" in result

    def test_no_results(self, mock_tool_context, mock_langchain_tavily):
        mock_instance = MagicMock()
        mock_instance.invoke.return_value = {"results": []}
        mock_langchain_tavily.TavilySearch.return_value = mock_instance

        tools = create_web_tools(mock_tool_context)
        ws = next(t for t in tools if t.name == "web_search")
        result = ws.invoke({"query": "obscure"})
        assert "No web results found" in result

    def test_quota_error_is_not_reported_as_no_results(
        self, mock_tool_context, mock_langchain_tavily
    ):
        mock_instance = MagicMock()
        mock_instance.invoke.return_value = {"error": "Error 432: usage limit exceeded"}
        mock_langchain_tavily.TavilySearch.return_value = mock_instance

        tools = create_web_tools(mock_tool_context)
        ws = next(t for t in tools if t.name == "web_search")
        result = ws.invoke({"query": "obscure"})

        assert "usage limit exceeded" in result
        assert "No web results found" not in result

    @pytest.mark.parametrize(
        "error",
        [
            ProviderAuthError("primary auth"),
            ProviderQuotaError("primary quota"),
            ProviderRateLimitError("primary rate limit"),
            ProviderUnavailableError("primary unavailable"),
        ],
    )
    def test_hard_provider_error_uses_fallback_once(self, error, caplog):
        primary = MagicMock(provider="tavily")
        primary.search.side_effect = error
        fallback = MagicMock(provider="searxng")
        fallback.search.return_value = [
            Result(
                title="Fallback result",
                url="https://example.com",
                snippet="answer",
            )
        ]

        with caplog.at_level("WARNING"):
            result = _direct_web_search(
                "query",
                5,
                adapter=primary,
                fallback_adapter=fallback,
            )

        fallback.search.assert_called_once()
        assert "Provider fallback: searxng answered after tavily failed." in result
        assert "Fallback result" in result
        assert str(error) in caplog.text

    def test_provider_request_error_does_not_use_fallback(self):
        primary = MagicMock(provider="tavily")
        primary.search.side_effect = ProviderRequestError("bad request")
        fallback = MagicMock(provider="searxng")

        result = _direct_web_search(
            "query",
            5,
            adapter=primary,
            fallback_adapter=fallback,
        )

        fallback.search.assert_not_called()
        assert "bad request" in result

    def test_empty_primary_result_does_not_use_fallback(self):
        primary = MagicMock(provider="tavily")
        primary.search.return_value = []
        fallback = MagicMock(provider="searxng")

        result = _direct_web_search(
            "query",
            5,
            adapter=primary,
            fallback_adapter=fallback,
        )

        fallback.search.assert_not_called()
        assert result == "No web results found for: query"

    def test_failing_fallback_surfaces_primary_error(self):
        primary = MagicMock(provider="tavily")
        primary.search.side_effect = ProviderQuotaError("primary quota")
        fallback = MagicMock(provider="searxng")
        fallback.search.side_effect = ProviderUnavailableError("fallback down")

        result = _direct_web_search(
            "query",
            5,
            adapter=primary,
            fallback_adapter=fallback,
        )

        fallback.search.assert_called_once()
        assert "primary quota" in result
        assert "fallback down" not in result

    def test_search_depth_advanced(self, mock_tool_context, mock_langchain_tavily):
        mock_instance = MagicMock()
        mock_instance.invoke.return_value = self._make_search_response()
        mock_langchain_tavily.TavilySearch.return_value = mock_instance

        tools = create_web_tools(mock_tool_context)
        ws = next(t for t in tools if t.name == "web_search")
        ws.invoke({"query": "test", "search_depth": "advanced"})

        call_kwargs = mock_instance.invoke.call_args[0][0]
        assert call_kwargs["search_depth"] == "advanced"

    def test_topic_news(self, mock_tool_context, mock_langchain_tavily):
        mock_instance = MagicMock()
        mock_instance.invoke.return_value = self._make_search_response()
        mock_langchain_tavily.TavilySearch.return_value = mock_instance

        tools = create_web_tools(mock_tool_context)
        ws = next(t for t in tools if t.name == "web_search")
        ws.invoke({"query": "test", "topic": "news"})

        call_kwargs = mock_instance.invoke.call_args[0][0]
        assert call_kwargs["topic"] == "news"

    def test_time_range(self, mock_tool_context, mock_langchain_tavily):
        mock_instance = MagicMock()
        mock_instance.invoke.return_value = self._make_search_response()
        mock_langchain_tavily.TavilySearch.return_value = mock_instance

        tools = create_web_tools(mock_tool_context)
        ws = next(t for t in tools if t.name == "web_search")
        ws.invoke({"query": "test", "time_range": "week"})

        call_kwargs = mock_instance.invoke.call_args[0][0]
        assert call_kwargs["time_range"] == "week"

    def test_include_domains(self, mock_tool_context, mock_langchain_tavily):
        mock_instance = MagicMock()
        mock_instance.invoke.return_value = self._make_search_response()
        mock_langchain_tavily.TavilySearch.return_value = mock_instance

        tools = create_web_tools(mock_tool_context)
        ws = next(t for t in tools if t.name == "web_search")
        ws.invoke({"query": "test", "include_domains": "example.com, other.com"})

        call_kwargs = mock_instance.invoke.call_args[0][0]
        assert call_kwargs["include_domains"] == ["example.com", "other.com"]

    def test_exclude_domains(self, mock_tool_context, mock_langchain_tavily):
        mock_instance = MagicMock()
        mock_instance.invoke.return_value = self._make_search_response()
        mock_langchain_tavily.TavilySearch.return_value = mock_instance

        tools = create_web_tools(mock_tool_context)
        ws = next(t for t in tools if t.name == "web_search")
        ws.invoke({"query": "test", "exclude_domains": "spam.com"})

        call_kwargs = mock_instance.invoke.call_args[0][0]
        assert call_kwargs["exclude_domains"] == ["spam.com"]

    def test_raw_content_creates_new_instance(
        self, mock_tool_context, mock_langchain_tavily
    ):
        mock_instance = MagicMock()
        mock_instance.invoke.return_value = self._make_search_response(include_raw=True)
        mock_langchain_tavily.TavilySearch.return_value = mock_instance

        tools = create_web_tools(mock_tool_context)
        ws = next(t for t in tools if t.name == "web_search")
        ws.invoke({"query": "test", "include_raw_content": True})

        constructor_kwargs = mock_langchain_tavily.TavilySearch.call_args[1]
        assert constructor_kwargs.get("include_raw_content") is True

    def test_raw_content_archived_not_inlined(
        self, mock_tool_context, mock_langchain_tavily
    ):
        long_content = "A" * 500
        mock_instance = MagicMock()
        mock_instance.invoke.return_value = {
            "results": [
                {
                    "url": "https://ex.com",
                    "title": "T",
                    "content": "short",
                    "raw_content": long_content,
                }
            ]
        }
        mock_langchain_tavily.TavilySearch.return_value = mock_instance

        tools = create_web_tools(mock_tool_context)
        ws = next(t for t in tools if t.name == "web_search")
        result = ws.invoke({"query": "test", "include_raw_content": True})

        assert long_content not in result
        assert "Snippet: short" in result
        assert "Full text saved:" in result

    def test_raw_content_not_inlined_even_when_huge(
        self, mock_tool_context, mock_langchain_tavily
    ):
        huge_content = "word " * 6000
        mock_instance = MagicMock()
        mock_instance.invoke.return_value = {
            "results": [
                {
                    "url": "https://ex.com",
                    "title": "T",
                    "content": "short",
                    "raw_content": huge_content,
                }
            ]
        }
        mock_langchain_tavily.TavilySearch.return_value = mock_instance

        tools = create_web_tools(mock_tool_context)
        ws = next(t for t in tools if t.name == "web_search")
        result = ws.invoke({"query": "test", "include_raw_content": True})

        assert "truncated from 6000 words" not in result
        assert "Full text saved:" in result

    def test_citation_registration(self, mock_tool_context, mock_langchain_tavily):
        mock_instance = MagicMock()
        mock_instance.invoke.return_value = self._make_search_response()
        mock_langchain_tavily.TavilySearch.return_value = mock_instance
        mock_tool_context.get_or_register_web_source.return_value = ("src-1", None)

        tools = create_web_tools(mock_tool_context)
        ws = next(t for t in tools if t.name == "web_search")
        result = ws.invoke({"query": "test"})

        assert mock_tool_context.get_or_register_web_source.called
        first_call = mock_tool_context.get_or_register_web_source.call_args_list[0]
        assert first_call.kwargs["content"] == "Short snippet for result 0"
        assert "archived" in result

    def test_inaccessible_sources_warning(
        self, mock_tool_context, mock_langchain_tavily
    ):
        mock_instance = MagicMock()
        mock_instance.invoke.return_value = self._make_search_response()
        mock_langchain_tavily.TavilySearch.return_value = mock_instance
        mock_tool_context.get_or_register_web_source.return_value = (
            "src-1",
            "HTTP 403",
        )

        tools = create_web_tools(mock_tool_context)
        ws = next(t for t in tools if t.name == "web_search")
        result = ws.invoke({"query": "test"})

        assert "WARNING" in result
        assert "INACCESSIBLE" in result

    def test_raw_search_archives_full_text_without_context_bloat(
        self, tmp_path, mock_langchain_tavily
    ):
        raw_content = "RAW_FULL_TEXT " + ("x" * 200_000)
        results = [
            {
                "url": f"https://example.com/page-{i}",
                "title": f"Result {i}",
                "content": f"Short snippet {i}",
                "raw_content": f"{raw_content} {i}",
            }
            for i in range(10)
        ]
        mock_instance = MagicMock()
        mock_instance.invoke.return_value = {"results": results}
        mock_langchain_tavily.TavilySearch.return_value = mock_instance
        context = _make_disk_context(tmp_path)

        output = _direct_web_search(
            "heavy query",
            10,
            context,
            include_raw_content=True,
        )

        assert len(output) < 15_000
        assert "RAW_FULL_TEXT" not in output
        assert output.count("Full text saved: documents/external/") == 10
        assert (
            mock_langchain_tavily.TavilySearch.call_args[1]["include_raw_content"]
            is True
        )
        saved_files = list((tmp_path / "documents" / "external").glob("*.md"))
        assert len(saved_files) == 10
        assert all("RAW_FULL_TEXT" in path.read_text() for path in saved_files)
        for path in saved_files:
            relative_path = path.relative_to(tmp_path).as_posix()
            assert relative_path in output

    def test_raw_search_without_workspace_uses_bounded_excerpts(
        self, mock_langchain_tavily
    ):
        results = [
            {
                "url": f"https://example.com/no-workspace-{i}",
                "title": f"Result {i}",
                "content": f"Short snippet {i}",
                "raw_content": f"RAW_{i} " + ("word " * 3000),
            }
            for i in range(10)
        ]
        mock_instance = MagicMock()
        mock_instance.invoke.return_value = {"results": results}
        mock_langchain_tavily.TavilySearch.return_value = mock_instance

        output = _direct_web_search(
            "heavy query",
            10,
            _make_no_workspace_context(),
            include_raw_content=True,
        )

        assert "Full text saved:" not in output
        assert "Content excerpt (not saved):" in output
        assert "inline aggregate cap reached" in output
        assert len(output) < MAX_TOTAL_INLINE_CHARS + 8_000

    def test_inaccessible_source_has_warning_without_saved_path_pointer(
        self, tmp_path, mock_langchain_tavily
    ):
        mock_instance = MagicMock()
        mock_instance.invoke.return_value = {
            "results": [
                {
                    "url": "https://blocked.example.com",
                    "title": "Blocked",
                    "content": "Blocked snippet",
                    "raw_content": "Blocked full text",
                }
            ]
        }
        mock_langchain_tavily.TavilySearch.return_value = mock_instance

        async def register_inaccessible(url, name=None, content=None):
            del url, name, content
            return 42, "HTTP 403"

        output = _direct_web_search(
            "blocked",
            1,
            _make_disk_context(tmp_path, register_inaccessible),
            include_raw_content=True,
        )

        assert "WARNING" in output
        assert "INACCESSIBLE" in output
        assert "Full text saved:" not in output
        assert "Content excerpt (not saved):" not in output


# ── provider errors vs. genuinely empty results ────────────────────


# Verbatim Tavily HTTP error messages as langchain_tavily's HTTP layer renders
# them (``ValueError(f"Error {status}: {detail['error']}")``).
TAVILY_ERRORS = {
    401: "Error 401: Unauthorized: missing or invalid API key.",
    429: "Error 429: Too many requests. Please try again later.",
    432: (
        "Error 432: This request exceeds this API key's set usage limit. "
        "You can increase its limit on the Tavily dashboard."
    ),
}

# What the wrappers really return for a genuinely empty result set: ``_run``
# raises ``ToolException`` and ``handle_tool_error=True`` renders it as a bare
# string, so ``invoke`` hands back text rather than a dict.
TAVILY_EMPTY_SEARCH = (
    "No search results found for 'obscure'. Suggestions: Try a more detailed "
    "search using 'advanced' search_depth. Try modifying your search parameters "
    "with one of these approaches."
)
TAVILY_EMPTY_EXTRACT = (
    "No extracted results found for '['https://a.com']'. Suggestions: Try a more "
    "detailed extraction using 'advanced' extract_depth."
)
TAVILY_EMPTY_CRAWL = (
    "No crawl results found for 'https://a.com'. Suggestions: . Try modifying "
    "your crawl parameters with one of these approaches."
)


def _returning(mock_langchain_tavily, tool_class_name, response):
    """Make ``langchain_tavily.<tool_class_name>().invoke`` return ``response``."""
    instance = MagicMock()
    instance.invoke.return_value = response
    getattr(mock_langchain_tavily, tool_class_name).return_value = instance
    return instance


def _tavily_adapter():
    """The real Tavily adapter over the mocked ``langchain_tavily`` module."""
    return TavilyAdapter(api_key="tvly-secret-key")


class TestWebSearchProviderErrors:
    """A provider failure must never be rendered as an empty result set.

    ``langchain_tavily`` does not raise on an HTTP failure: it returns
    ``{"error": <exception>}`` (older releases: the message string) with no
    ``results`` key. Only a real empty set may say "No web results found".
    """

    def test_error_dict_with_exception_is_surfaced(self, mock_langchain_tavily):
        _returning(
            mock_langchain_tavily,
            "TavilySearch",
            {"error": ValueError(TAVILY_ERRORS[432])},
        )

        result = _direct_web_search(
            "Michelstadt Vereine", 5, None, adapter=_tavily_adapter()
        )

        assert result.startswith("Web search failed:")
        assert "Error 432" in result
        assert "usage limit" in result
        assert "No web results" not in result

    def test_error_dict_with_string_is_surfaced(self, mock_langchain_tavily):
        _returning(mock_langchain_tavily, "TavilySearch", {"error": TAVILY_ERRORS[432]})

        result = _direct_web_search("q", 5, None, adapter=_tavily_adapter())

        assert result.startswith("Web search failed:")
        assert "usage limit" in result
        assert "No web results" not in result

    @pytest.mark.parametrize("status", [401, 429, 432])
    def test_http_error_statuses_are_surfaced(self, mock_langchain_tavily, status):
        _returning(
            mock_langchain_tavily,
            "TavilySearch",
            {"error": ValueError(TAVILY_ERRORS[status])},
        )

        result = _direct_web_search("q", 5, None, adapter=_tavily_adapter())

        assert result.startswith("Web search failed:")
        assert f"Error {status}" in result
        assert "No web results" not in result

    def test_error_logs_warning_with_provider_and_status_but_never_the_key(
        self, mock_langchain_tavily, caplog
    ):
        _returning(
            mock_langchain_tavily,
            "TavilySearch",
            {"error": ValueError("Error 401: rejected key tvly-secret-key")},
        )

        with caplog.at_level(logging.WARNING):
            result = _direct_web_search("q", 5, None, adapter=_tavily_adapter())

        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert warnings, "expected a WARNING log for the provider failure"
        logged = warnings[-1].getMessage()
        assert "tavily" in logged.lower()
        assert "401" in logged
        assert "tvly-secret-key" not in logged
        assert result.startswith("Web search failed:")
        assert "tvly-secret-key" not in result

    def test_error_skips_source_registration(
        self, mock_tool_context, mock_langchain_tavily
    ):
        _returning(
            mock_langchain_tavily,
            "TavilySearch",
            {"error": ValueError(TAVILY_ERRORS[429])},
        )

        tools = create_web_tools(mock_tool_context)
        ws = next(t for t in tools if t.name == "web_search")
        result = ws.invoke({"query": "q"})

        assert result.startswith("Web search failed:")
        mock_tool_context.get_or_register_web_source.assert_not_called()

    def test_empty_results_list_is_reported_as_no_results(self, mock_langchain_tavily):
        _returning(mock_langchain_tavily, "TavilySearch", {"results": []})

        result = _direct_web_search("obscure", 5, None, adapter=_tavily_adapter())

        assert result == "No web results found for: obscure"

    def test_library_empty_notice_string_is_reported_as_no_results(
        self, mock_langchain_tavily
    ):
        _returning(mock_langchain_tavily, "TavilySearch", TAVILY_EMPTY_SEARCH)

        result = _direct_web_search("obscure", 5, None, adapter=_tavily_adapter())

        assert result == "No web results found for: obscure"

    def test_library_empty_notice_does_not_fail_over(self, mock_langchain_tavily):
        # An empty set is an answer; it must not be mistaken for an outage and
        # burn a call on the fallback provider.
        _returning(mock_langchain_tavily, "TavilySearch", TAVILY_EMPTY_SEARCH)
        fallback = MagicMock(provider="searxng")

        result = _direct_web_search(
            "obscure",
            5,
            None,
            adapter=_tavily_adapter(),
            fallback_adapter=fallback,
        )

        fallback.search.assert_not_called()
        assert result == "No web results found for: obscure"

    def test_normal_payload_is_unchanged(self, mock_langchain_tavily):
        _returning(
            mock_langchain_tavily,
            "TavilySearch",
            {
                "results": [
                    {
                        "url": "https://example0.com",
                        "title": "Result 0",
                        "content": "Short snippet for result 0",
                    },
                    {
                        "url": "https://example1.com",
                        "title": "Result 1",
                        "content": "Short snippet for result 1",
                    },
                ]
            },
        )

        result = _direct_web_search("test query", 5, None, adapter=_tavily_adapter())

        assert result.startswith("Web Search Results for: test query\nResults: 2\n\n")
        assert (
            "1. Result 0\n   URL: https://example0.com\n"
            "   Snippet: Short snippet for result 0\n" in result
        )
        assert "2. Result 1\n   URL: https://example1.com\n" in result
        assert "failed" not in result.lower()


class TestSiblingProviderErrors:
    """Extract / Crawl / Map share the wrapper's in-band error shape."""

    @staticmethod
    def _provider_warning(caplog, status):
        return any(
            "tavily" in r.getMessage().lower() and str(status) in r.getMessage()
            for r in caplog.records
            if r.levelno == logging.WARNING
        )

    def test_extract_error_is_surfaced(self, mock_langchain_tavily, caplog):
        _returning(
            mock_langchain_tavily,
            "TavilyExtract",
            {"error": ValueError(TAVILY_ERRORS[401])},
        )

        with caplog.at_level(logging.WARNING):
            result = _extract_webpage("https://a.com", None, adapter=_tavily_adapter())

        assert result.startswith("Webpage extraction failed:")
        assert "Error 401" in result
        assert "No content could be extracted" not in result
        assert self._provider_warning(caplog, 401)

    def test_extract_library_empty_notice_is_reported_as_no_content(
        self, mock_langchain_tavily
    ):
        _returning(mock_langchain_tavily, "TavilyExtract", TAVILY_EMPTY_EXTRACT)

        result = _extract_webpage("https://a.com", None, adapter=_tavily_adapter())

        assert result == "No content could be extracted from the provided URL(s)."

    def test_crawl_error_is_surfaced(self, mock_langchain_tavily, caplog):
        _returning(
            mock_langchain_tavily,
            "TavilyCrawl",
            {"error": ValueError(TAVILY_ERRORS[429])},
        )

        with caplog.at_level(logging.WARNING):
            result = _crawl_website("https://a.com", None, adapter=_tavily_adapter())

        assert result.startswith("Website crawl failed:")
        assert "Error 429" in result
        assert "No pages could be crawled" not in result
        assert self._provider_warning(caplog, 429)

    def test_crawl_library_empty_notice_is_reported_as_no_pages(
        self, mock_langchain_tavily
    ):
        _returning(mock_langchain_tavily, "TavilyCrawl", TAVILY_EMPTY_CRAWL)

        result = _crawl_website("https://a.com", None, adapter=_tavily_adapter())

        assert result == "No pages could be crawled from: https://a.com"

    def test_map_error_is_surfaced(self, mock_langchain_tavily, caplog):
        _returning(
            mock_langchain_tavily,
            "TavilyMap",
            {"error": ValueError(TAVILY_ERRORS[432])},
        )

        with caplog.at_level(logging.WARNING):
            result = _map_website("https://a.com", adapter=_tavily_adapter())

        assert result.startswith("Website map failed:")
        assert "Error 432" in result
        assert "No URLs discovered" not in result
        assert self._provider_warning(caplog, 432)

    def test_map_library_empty_notice_is_reported_as_no_urls(
        self, mock_langchain_tavily
    ):
        # TavilyMap reuses the crawl wording for its empty-result notice.
        _returning(mock_langchain_tavily, "TavilyMap", TAVILY_EMPTY_CRAWL)

        result = _map_website("https://a.com", adapter=_tavily_adapter())

        assert result == "No URLs discovered for: https://a.com"


# ── extract_webpage ────────────────────────────────────────────────


class TestExtractWebpage:
    """Tests for the extract_webpage tool."""

    def _setup_extract(self, mock_langchain_tavily, response=None):
        mock_instance = MagicMock()
        if response is None:
            response = {
                "results": [
                    {
                        "url": "https://example.com/page1",
                        "raw_content": "Full page content here",
                    },
                ],
                "failed_results": [],
            }
        mock_instance.invoke.return_value = response
        mock_langchain_tavily.TavilyExtract.return_value = mock_instance
        return mock_instance

    def test_single_url(self, mock_tool_context, mock_langchain_tavily):
        self._setup_extract(mock_langchain_tavily)
        mock_tool_context.get_or_register_web_source.return_value = ("src-1", None)

        tools = create_web_tools(mock_tool_context)
        t = next(t for t in tools if t.name == "extract_webpage")
        result = t.invoke({"urls": "https://example.com/page1"})

        assert "Extracted Content from 1 URL(s)" in result
        assert "Full page content here" in result

    def test_multiple_urls(self, mock_tool_context, mock_langchain_tavily):
        mock_instance = self._setup_extract(
            mock_langchain_tavily,
            {
                "results": [
                    {"url": "https://a.com", "raw_content": "Content A"},
                    {"url": "https://b.com", "raw_content": "Content B"},
                ],
                "failed_results": [],
            },
        )

        tools = create_web_tools(mock_tool_context)
        t = next(t for t in tools if t.name == "extract_webpage")
        result = t.invoke({"urls": "https://a.com, https://b.com"})

        call_kwargs = mock_instance.invoke.call_args[0][0]
        assert call_kwargs["urls"] == ["https://a.com", "https://b.com"]
        assert "Content A" in result
        assert "Content B" in result

    def test_with_query(self, mock_tool_context, mock_langchain_tavily):
        mock_instance = self._setup_extract(mock_langchain_tavily)

        tools = create_web_tools(mock_tool_context)
        t = next(t for t in tools if t.name == "extract_webpage")
        t.invoke({"urls": "https://example.com", "query": "important info"})

        call_kwargs = mock_instance.invoke.call_args[0][0]
        assert call_kwargs["query"] == "important info"

    def test_advanced_depth(self, mock_tool_context, mock_langchain_tavily):
        self._setup_extract(mock_langchain_tavily)

        tools = create_web_tools(mock_tool_context)
        t = next(t for t in tools if t.name == "extract_webpage")
        t.invoke({"urls": "https://example.com", "extract_depth": "advanced"})

        assert (
            mock_langchain_tavily.TavilyExtract.call_args[1]["extract_depth"]
            == "advanced"
        )

    def test_citation_registration(self, mock_tool_context, mock_langchain_tavily):
        self._setup_extract(mock_langchain_tavily)
        mock_tool_context.get_or_register_web_source.return_value = ("src-1", None)

        tools = create_web_tools(mock_tool_context)
        t = next(t for t in tools if t.name == "extract_webpage")
        result = t.invoke({"urls": "https://example.com/page1"})

        mock_tool_context.get_or_register_web_source.assert_called_with(
            "https://example.com/page1",
            content="Full page content here",
        )
        assert "archived" in result

    def test_missing_api_key(self, mock_tool_context):
        _remove_tavily_key(mock_tool_context)
        tools = create_web_tools(mock_tool_context)
        t = next(t for t in tools if t.name == "extract_webpage")
        result = t.invoke({"urls": "https://example.com"})
        assert "Tavily API key is not configured" in result

    def test_content_word_limit(self, mock_tool_context, mock_langchain_tavily):
        huge = "word " * 6000
        self._setup_extract(
            mock_langchain_tavily,
            {
                "results": [{"url": "https://ex.com", "raw_content": huge}],
                "failed_results": [],
            },
        )

        tools = create_web_tools(mock_tool_context)
        t = next(t for t in tools if t.name == "extract_webpage")
        result = t.invoke({"urls": "https://ex.com"})

        assert "truncated from 6000 words" in result

    def test_failed_urls_reported(self, mock_tool_context, mock_langchain_tavily):
        self._setup_extract(
            mock_langchain_tavily,
            {
                "results": [{"url": "https://a.com", "raw_content": "OK"}],
                "failed_results": ["https://bad.com"],
            },
        )

        tools = create_web_tools(mock_tool_context)
        t = next(t for t in tools if t.name == "extract_webpage")
        result = t.invoke({"urls": "https://a.com, https://bad.com"})

        assert "1 failed" in result
        assert "https://bad.com" in result

    def test_too_many_urls(self, mock_tool_context):
        tools = create_web_tools(mock_tool_context)
        t = next(t for t in tools if t.name == "extract_webpage")
        urls = ", ".join(f"https://url{i}.com" for i in range(21))
        result = t.invoke({"urls": urls})
        assert "Maximum 20 URLs" in result

    def test_empty_urls(self, mock_tool_context):
        tools = create_web_tools(mock_tool_context)
        t = next(t for t in tools if t.name == "extract_webpage")
        result = t.invoke({"urls": ""})
        assert "No URLs provided" in result

    def test_many_huge_urls_use_aggregate_cap_then_saved_pointers(
        self, tmp_path, mock_langchain_tavily
    ):
        results = [
            {
                "url": f"https://example.com/extract-{i}",
                "raw_content": f"START_{i} " + ("word " * 3000) + f"TAIL_{i}",
            }
            for i in range(20)
        ]
        self._setup_extract(
            mock_langchain_tavily,
            {"results": results, "failed_results": []},
        )

        output = _extract_webpage(
            ",".join(result["url"] for result in results),
            _make_disk_context(tmp_path),
        )

        assert "TAIL_0" in output
        assert "TAIL_10" not in output
        assert output.count("Full text saved: documents/external/") >= 10
        assert len(list((tmp_path / "documents" / "external").glob("*.md"))) == 20


# ── crawl_website ──────────────────────────────────────────────────


class TestCrawlWebsite:
    """Tests for the crawl_website tool."""

    def _setup_crawl(self, mock_langchain_tavily, response=None):
        mock_instance = MagicMock()
        if response is None:
            response = {
                "results": [
                    {
                        "url": "https://docs.example.com/",
                        "raw_content": "Homepage content",
                    },
                    {
                        "url": "https://docs.example.com/page2",
                        "raw_content": "Page 2 content",
                    },
                ],
            }
        mock_instance.invoke.return_value = response
        mock_langchain_tavily.TavilyCrawl.return_value = mock_instance
        return mock_instance

    def test_basic_crawl(self, mock_tool_context, mock_langchain_tavily):
        self._setup_crawl(mock_langchain_tavily)
        mock_tool_context.get_or_register_web_source.return_value = ("src-1", None)

        tools = create_web_tools(mock_tool_context)
        t = next(t for t in tools if t.name == "crawl_website")
        result = t.invoke({"url": "https://docs.example.com/"})

        assert "Website Crawl Results" in result
        assert "Pages crawled: 2" in result
        assert "Homepage content" in result

    def test_with_instructions(self, mock_tool_context, mock_langchain_tavily):
        mock_instance = self._setup_crawl(mock_langchain_tavily)

        tools = create_web_tools(mock_tool_context)
        t = next(t for t in tools if t.name == "crawl_website")
        t.invoke({"url": "https://docs.example.com/", "instructions": "find API docs"})

        call_kwargs = mock_instance.invoke.call_args[0][0]
        assert call_kwargs["instructions"] == "find API docs"

    def test_path_filters(self, mock_tool_context, mock_langchain_tavily):
        mock_instance = self._setup_crawl(mock_langchain_tavily)

        tools = create_web_tools(mock_tool_context)
        t = next(t for t in tools if t.name == "crawl_website")
        t.invoke(
            {
                "url": "https://docs.example.com/",
                "select_paths": "/api/.*, /docs/.*",
                "exclude_paths": "/blog/.*",
            }
        )

        call_kwargs = mock_instance.invoke.call_args[0][0]
        assert call_kwargs["select_paths"] == ["/api/.*", "/docs/.*"]
        assert call_kwargs["exclude_paths"] == ["/blog/.*"]

    def test_depth_clamping_high(self, mock_tool_context, mock_langchain_tavily):
        mock_instance = self._setup_crawl(mock_langchain_tavily)

        tools = create_web_tools(mock_tool_context)
        t = next(t for t in tools if t.name == "crawl_website")
        t.invoke({"url": "https://example.com", "max_depth": 10})

        call_kwargs = mock_instance.invoke.call_args[0][0]
        assert call_kwargs["max_depth"] == 5

    def test_depth_clamping_low(self, mock_tool_context, mock_langchain_tavily):
        mock_instance = self._setup_crawl(mock_langchain_tavily)

        tools = create_web_tools(mock_tool_context)
        t = next(t for t in tools if t.name == "crawl_website")
        t.invoke({"url": "https://example.com", "max_depth": -1})

        call_kwargs = mock_instance.invoke.call_args[0][0]
        assert call_kwargs["max_depth"] == 1

    def test_citation_registration(self, mock_tool_context, mock_langchain_tavily):
        self._setup_crawl(mock_langchain_tavily)
        mock_tool_context.get_or_register_web_source.return_value = ("src-1", None)

        tools = create_web_tools(mock_tool_context)
        t = next(t for t in tools if t.name == "crawl_website")
        result = t.invoke({"url": "https://docs.example.com/"})

        assert mock_tool_context.get_or_register_web_source.call_count == 2
        assert (
            mock_tool_context.get_or_register_web_source.call_args_list[0].kwargs[
                "content"
            ]
            == "Homepage content"
        )
        assert "archived" in result

    def test_missing_api_key(self, mock_tool_context):
        _remove_tavily_key(mock_tool_context)
        tools = create_web_tools(mock_tool_context)
        t = next(t for t in tools if t.name == "crawl_website")
        result = t.invoke({"url": "https://example.com"})
        assert "Tavily API key is not configured" in result

    def test_content_word_limit(self, mock_tool_context, mock_langchain_tavily):
        huge = "word " * 6000
        self._setup_crawl(
            mock_langchain_tavily,
            {
                "results": [{"url": "https://ex.com", "raw_content": huge}],
            },
        )

        tools = create_web_tools(mock_tool_context)
        t = next(t for t in tools if t.name == "crawl_website")
        result = t.invoke({"url": "https://ex.com"})

        assert "truncated from 6000 words" not in result
        assert "Full text saved:" in result

    def test_no_results(self, mock_tool_context, mock_langchain_tavily):
        self._setup_crawl(mock_langchain_tavily, {"results": []})

        tools = create_web_tools(mock_tool_context)
        t = next(t for t in tools if t.name == "crawl_website")
        result = t.invoke({"url": "https://example.com"})

        assert "No pages could be crawled" in result

    def test_crawl_returns_snippet_and_saved_pointer_per_page(
        self, tmp_path, mock_langchain_tavily
    ):
        self._setup_crawl(
            mock_langchain_tavily,
            {
                "results": [
                    {
                        "url": "https://docs.example.com/page",
                        "raw_content": ("A" * 800) + "TAIL_SHOULD_NOT_INLINE",
                    }
                ],
            },
        )

        output = _crawl_website(
            "https://docs.example.com",
            _make_disk_context(tmp_path),
        )

        assert "Full text saved: documents/external/" in output
        assert "TAIL_SHOULD_NOT_INLINE" not in output
        assert len(list((tmp_path / "documents" / "external").glob("*.md"))) == 1


# ── map_website ────────────────────────────────────────────────────


class TestMapWebsite:
    """Tests for the map_website tool."""

    def _setup_map(self, mock_langchain_tavily, response=None):
        mock_instance = MagicMock()
        if response is None:
            response = {
                "results": [
                    "https://example.com/",
                    "https://example.com/about",
                    "https://example.com/docs",
                ],
            }
        mock_instance.invoke.return_value = response
        mock_langchain_tavily.TavilyMap.return_value = mock_instance
        return mock_instance

    def test_basic_map(self, mock_tool_context, mock_langchain_tavily):
        self._setup_map(mock_langchain_tavily)

        tools = create_web_tools(mock_tool_context)
        t = next(t for t in tools if t.name == "map_website")
        result = t.invoke({"url": "https://example.com"})

        assert "Website Map for: https://example.com" in result
        assert "URLs discovered: 3" in result
        assert "https://example.com/about" in result

    def test_with_instructions(self, mock_tool_context, mock_langchain_tavily):
        mock_instance = self._setup_map(mock_langchain_tavily)

        tools = create_web_tools(mock_tool_context)
        t = next(t for t in tools if t.name == "map_website")
        t.invoke({"url": "https://example.com", "instructions": "find docs"})

        call_kwargs = mock_instance.invoke.call_args[0][0]
        assert call_kwargs["instructions"] == "find docs"

    def test_path_filters(self, mock_tool_context, mock_langchain_tavily):
        mock_instance = self._setup_map(mock_langchain_tavily)

        tools = create_web_tools(mock_tool_context)
        t = next(t for t in tools if t.name == "map_website")
        t.invoke(
            {
                "url": "https://example.com",
                "select_paths": "/docs/.*",
                "exclude_paths": "/blog/.*",
            }
        )

        call_kwargs = mock_instance.invoke.call_args[0][0]
        assert call_kwargs["select_paths"] == ["/docs/.*"]
        assert call_kwargs["exclude_paths"] == ["/blog/.*"]

    def test_no_citation_registration(self, mock_tool_context, mock_langchain_tavily):
        self._setup_map(mock_langchain_tavily)

        tools = create_web_tools(mock_tool_context)
        t = next(t for t in tools if t.name == "map_website")
        t.invoke({"url": "https://example.com"})

        mock_tool_context.get_or_register_web_source.assert_not_called()

    def test_missing_api_key(self, mock_tool_context):
        _remove_tavily_key(mock_tool_context)
        tools = create_web_tools(mock_tool_context)
        t = next(t for t in tools if t.name == "map_website")
        result = t.invoke({"url": "https://example.com"})
        assert "Tavily API key is not configured" in result

    def test_guidance_message(self, mock_tool_context, mock_langchain_tavily):
        self._setup_map(mock_langchain_tavily)

        tools = create_web_tools(mock_tool_context)
        t = next(t for t in tools if t.name == "map_website")
        result = t.invoke({"url": "https://example.com"})

        assert "extract_webpage" in result
        assert "crawl_website" in result

    def test_dict_results(self, mock_tool_context, mock_langchain_tavily):
        """Test handling when results are dicts instead of strings."""
        self._setup_map(
            mock_langchain_tavily,
            {
                "results": [
                    {"url": "https://example.com/page1"},
                    {"url": "https://example.com/page2"},
                ],
            },
        )

        tools = create_web_tools(mock_tool_context)
        t = next(t for t in tools if t.name == "map_website")
        result = t.invoke({"url": "https://example.com"})

        assert "https://example.com/page1" in result
        assert "https://example.com/page2" in result

    def test_no_results(self, mock_tool_context, mock_langchain_tavily):
        self._setup_map(mock_langchain_tavily, {"results": []})

        tools = create_web_tools(mock_tool_context)
        t = next(t for t in tools if t.name == "map_website")
        result = t.invoke({"url": "https://example.com"})

        assert "No URLs discovered" in result

    def test_depth_clamping(self, mock_tool_context, mock_langchain_tavily):
        mock_instance = self._setup_map(mock_langchain_tavily)

        tools = create_web_tools(mock_tool_context)
        t = next(t for t in tools if t.name == "map_website")
        t.invoke({"url": "https://example.com", "max_depth": 99})

        call_kwargs = mock_instance.invoke.call_args[0][0]
        assert call_kwargs["max_depth"] == 5
