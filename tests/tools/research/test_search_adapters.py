"""Shared contracts and error classification for web-provider adapters."""

from types import ModuleType
from unittest.mock import MagicMock, patch

import httpx
import pytest

from agent.tools.research.search import (
    ADAPTER_NAMES,
    BraveAdapter,
    Crawl4AIAdapter,
    FirecrawlAdapter,
    Page,
    ProviderAuthError,
    ProviderQuotaError,
    ProviderRateLimitError,
    ProviderRequestError,
    ProviderUnavailableError,
    Result,
    SearxngAdapter,
    TavilyAdapter,
    create_search_adapter,
)


@pytest.fixture
def tavily_module():
    module = ModuleType("langchain_tavily")
    module.TavilySearch = MagicMock()
    module.TavilyExtract = MagicMock()
    module.TavilyCrawl = MagicMock()
    module.TavilyMap = MagicMock()
    with patch.dict("sys.modules", {"langchain_tavily": module}):
        yield module


def _client(module, name, response):
    instance = MagicMock()
    instance.invoke.return_value = response
    getattr(module, name).return_value = instance
    return instance


def _http_client(response: httpx.Response):
    client = MagicMock()
    client.get.return_value = response
    manager = MagicMock()
    manager.__enter__.return_value = client
    manager.__exit__.return_value = False
    return manager, client


def test_tavily_declared_ops_return_normalized_shapes(tavily_module):
    _client(
        tavily_module,
        "TavilySearch",
        {
            "results": [
                {
                    "title": "One",
                    "url": "https://example.com/one",
                    "content": "Snippet",
                    "raw_content": "Body",
                }
            ]
        },
    )
    _client(
        tavily_module,
        "TavilyExtract",
        {
            "results": [{"url": "https://example.com/one", "raw_content": "Body"}],
            "failed_results": ["https://example.com/bad"],
        },
    )
    _client(
        tavily_module,
        "TavilyCrawl",
        {"results": [{"url": "https://example.com/two", "raw_content": "Page"}]},
    )
    _client(
        tavily_module,
        "TavilyMap",
        {"results": ["https://example.com/one", {"url": "https://example.com/two"}]},
    )
    adapter = TavilyAdapter(api_key="tvly-test")

    assert adapter.ops == frozenset({"search", "extract", "crawl", "map"})
    assert adapter.search("query", 5) == [
        Result(
            title="One",
            url="https://example.com/one",
            snippet="Snippet",
            raw_content="Body",
        )
    ]
    assert adapter.extract(["https://example.com/one"]) == [
        Page(url="https://example.com/one", content="Body"),
        Page(url="https://example.com/bad", content="", failed="failed"),
    ]
    assert adapter.crawl("https://example.com") == [
        Page(url="https://example.com/two", content="Page")
    ]
    assert adapter.map("https://example.com") == [
        "https://example.com/one",
        "https://example.com/two",
    ]


@pytest.mark.parametrize("op", ["extract", "crawl", "map"])
def test_tavily_undeclared_ops_raise(op):
    adapter = TavilyAdapter(api_key="tvly-test", ops=frozenset({"search"}))

    with pytest.raises(ProviderRequestError):
        if op == "extract":
            adapter.extract(["https://example.com"])
        else:
            getattr(adapter, op)("https://example.com")


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        ("Error 401: invalid API key", ProviderAuthError),
        ("Error 403: forbidden", ProviderAuthError),
        ("Error 402: payment required", ProviderQuotaError),
        ("Error 432: usage limit exceeded", ProviderQuotaError),
        ("Error 429: rate limit exceeded", ProviderRateLimitError),
        ("Error 503: unavailable", ProviderUnavailableError),
        ("Error 400: invalid request", ProviderRequestError),
    ],
)
def test_tavily_response_errors_are_classified(tavily_module, error, expected):
    _client(tavily_module, "TavilySearch", {"error": error})
    adapter = TavilyAdapter(api_key="tvly-test")

    with pytest.raises(expected):
        adapter.search("query", 5)


def test_tavily_432_regression_surfaces_quota_error(tavily_module):
    _client(
        tavily_module,
        "TavilySearch",
        {"error": "Error 432: This request exceeds your plan's usage limit"},
    )
    adapter = TavilyAdapter(api_key="tvly-test")

    with pytest.raises(ProviderQuotaError, match="usage limit"):
        adapter.search("query", 5)


def test_tavily_zero_results_is_an_answer(tavily_module):
    _client(tavily_module, "TavilySearch", {"results": []})

    assert TavilyAdapter(api_key="tvly-test").search("query", 5) == []


def test_tavily_error_carried_as_exception_object_is_classified(tavily_module):
    # langchain_tavily 0.2.x returns the HTTP layer's exception itself, not its
    # text: {"error": ValueError("Error 432: ...")}.
    _client(
        tavily_module,
        "TavilySearch",
        {
            "error": ValueError(
                "Error 432: This request exceeds this API key's set usage limit."
            )
        },
    )

    with pytest.raises(ProviderQuotaError, match="usage limit") as raised:
        TavilyAdapter(api_key="tvly-test").search("query", 5)
    assert raised.value.status_code == 432


@pytest.mark.parametrize(
    ("op", "client", "notice"),
    [
        (
            "search",
            "TavilySearch",
            "No search results found for 'query'. Suggestions: Try a more "
            "detailed search using 'advanced' search_depth.",
        ),
        (
            "extract",
            "TavilyExtract",
            "No extracted results found for '['https://example.com']'. "
            "Suggestions: Try a more detailed extraction.",
        ),
        (
            "crawl",
            "TavilyCrawl",
            "No crawl results found for 'https://example.com'. Suggestions: .",
        ),
        # TavilyMap reuses the crawl wording for its empty-result notice.
        (
            "map",
            "TavilyMap",
            "No crawl results found for 'https://example.com'. Suggestions: .",
        ),
    ],
)
def test_tavily_empty_notice_string_is_an_answer_not_a_failure(
    tavily_module, op, client, notice
):
    # The wrappers raise ToolException for an empty result set and ship with
    # handle_tool_error=True, so invoke() returns that notice as a bare string.
    # A real empty set never arrives as a dict; it must not become a failover-
    # eligible ProviderUnavailableError.
    _client(tavily_module, client, notice)
    adapter = TavilyAdapter(api_key="tvly-test")

    if op == "search":
        assert adapter.search("query", 5) == []
    elif op == "extract":
        assert adapter.extract(["https://example.com"]) == []
    else:
        assert getattr(adapter, op)("https://example.com") == []


def test_tavily_error_message_never_carries_the_api_key(tavily_module):
    _client(
        tavily_module,
        "TavilySearch",
        {"error": "Error 401: key tvly-secret rejected"},
    )

    with pytest.raises(ProviderAuthError) as raised:
        TavilyAdapter(api_key="tvly-secret").search("query", 5)
    assert "tvly-secret" not in str(raised.value)
    assert "***" in str(raised.value)


def test_model_supplied_fetch_url_is_only_a_provider_argument(tavily_module):
    instance = _client(
        tavily_module,
        "TavilyExtract",
        {"results": [], "failed_results": []},
    )
    adapter = TavilyAdapter(
        api_key="tvly-test",
        base_url="https://configured-provider.example/api",
    )

    adapter.extract(["http://169.254.169.254/latest/meta-data"])

    # The adapter constructs only Tavily's typed client. The influenced URL is
    # payload for the off-pod provider and never an in-process request origin.
    tavily_module.TavilyExtract.assert_called_once_with(
        api_key="tvly-test", extract_depth="basic"
    )
    assert instance.invoke.call_args.args[0]["urls"] == [
        "http://169.254.169.254/latest/meta-data"
    ]


def test_searxng_search_returns_normalized_results():
    response = httpx.Response(
        200,
        json={
            "results": [
                {
                    "title": "SearX result",
                    "url": "https://result.example/page",
                    "content": "Snippet",
                }
            ]
        },
        request=httpx.Request("GET", "https://search.internal/search"),
    )
    manager, client = _http_client(response)
    adapter = SearxngAdapter(base_url="https://search.internal")

    with patch(
        "agent.tools.research.search.searxng.httpx.Client", return_value=manager
    ):
        results = adapter.search("query", 5)

    assert results == [
        Result(
            title="SearX result",
            url="https://result.example/page",
            snippet="Snippet",
        )
    ]
    assert client.get.call_args.args[0] == "https://search.internal/search"


def _searxng_search(payload: dict):
    response = httpx.Response(
        200,
        json=payload,
        request=httpx.Request("GET", "https://search.internal/search"),
    )
    manager, _ = _http_client(response)
    adapter = SearxngAdapter(base_url="https://search.internal")
    with patch(
        "agent.tools.research.search.searxng.httpx.Client", return_value=manager
    ):
        return adapter.search("query", 5)


def test_searxng_empty_answer_from_failed_engines_is_an_outage():
    # Observed on dev 2026-09-24: every upstream engine CAPTCHA-blocked or
    # rate-limited, yet SearXNG answers 200 with an empty result list.
    with pytest.raises(ProviderUnavailableError) as excinfo:
        _searxng_search(
            {
                "results": [],
                "unresponsive_engines": [
                    ["brave", "Suspended: too many requests"],
                    ["duckduckgo", "CAPTCHA"],
                ],
            }
        )

    assert excinfo.value.failover_eligible
    assert "brave (Suspended: too many requests)" in str(excinfo.value)
    assert "duckduckgo (CAPTCHA)" in str(excinfo.value)


def test_searxng_outage_message_is_bounded():
    unresponsive = [[f"engine{i}", "CAPTCHA"] for i in range(8)]

    with pytest.raises(ProviderUnavailableError) as excinfo:
        _searxng_search({"results": [], "unresponsive_engines": unresponsive})

    assert "8 upstream engine(s) failed" in str(excinfo.value)
    assert "engine5" not in str(excinfo.value)
    assert "3 more" in str(excinfo.value)


def test_searxng_zero_results_without_failed_engines_is_an_answer():
    assert _searxng_search({"results": [], "unresponsive_engines": []}) == []
    assert _searxng_search({"results": []}) == []


def test_searxng_results_win_over_partially_failed_engines():
    results = _searxng_search(
        {
            "results": [{"title": "Hit", "url": "https://result.example/"}],
            "unresponsive_engines": [["brave", "too many requests"]],
        }
    )

    assert [item.url for item in results] == ["https://result.example/"]


def test_brave_search_returns_normalized_results():
    response = httpx.Response(
        200,
        json={
            "web": {
                "results": [
                    {
                        "title": "Brave result",
                        "url": "https://result.example/page",
                        "description": "Snippet",
                    }
                ]
            }
        },
        request=httpx.Request("GET", "https://brave.internal/res/v1/web/search"),
    )
    manager, client = _http_client(response)
    adapter = BraveAdapter(base_url="https://brave.internal", api_key="brave-key")

    with patch("agent.tools.research.search.brave.httpx.Client", return_value=manager):
        results = adapter.search("query", 5, time_range="week")

    assert results == [
        Result(
            title="Brave result",
            url="https://result.example/page",
            snippet="Snippet",
        )
    ]
    assert client.get.call_args.args[0] == ("https://brave.internal/res/v1/web/search")
    assert client.get.call_args.kwargs["params"]["freshness"] == "pw"


@pytest.mark.parametrize(
    "adapter",
    [
        SearxngAdapter(base_url="https://search.internal"),
        BraveAdapter(base_url="https://brave.internal", api_key="brave-key"),
    ],
)
@pytest.mark.parametrize("op", ["extract", "crawl", "map"])
def test_search_only_adapters_raise_for_undeclared_ops(adapter, op):
    with pytest.raises(ProviderRequestError):
        if op == "extract":
            adapter.extract(["https://example.com"])
        else:
            getattr(adapter, op)("https://example.com")


@pytest.mark.parametrize(
    ("adapter", "module_path"),
    [
        (
            SearxngAdapter(base_url="https://search.internal"),
            "agent.tools.research.search.searxng.httpx.Client",
        ),
        (
            BraveAdapter(base_url="https://brave.internal", api_key="brave-key"),
            "agent.tools.research.search.brave.httpx.Client",
        ),
    ],
)
def test_http_search_adapters_classify_rate_limits(adapter, module_path):
    response = httpx.Response(
        429,
        request=httpx.Request("GET", "https://provider.internal/search"),
    )
    manager, _ = _http_client(response)

    with (
        patch(module_path, return_value=manager),
        pytest.raises(ProviderRateLimitError),
    ):
        adapter.search("query", 5)


@pytest.mark.parametrize(
    ("adapter", "module_path", "expected_origin"),
    [
        (
            SearxngAdapter(base_url="https://search.internal/root"),
            "agent.tools.research.search.searxng.httpx.Client",
            "https://search.internal/root/search",
        ),
        (
            BraveAdapter(base_url="https://brave.internal/api", api_key="brave-key"),
            "agent.tools.research.search.brave.httpx.Client",
            "https://brave.internal/api/res/v1/web/search",
        ),
    ],
)
def test_search_query_never_changes_http_request_origin(
    adapter, module_path, expected_origin
):
    response = httpx.Response(
        200,
        json={"results": []} if adapter.provider == "searxng" else {"web": {}},
        request=httpx.Request("GET", expected_origin),
    )
    manager, client = _http_client(response)
    influenced_query = "http://169.254.169.254/latest/meta-data"

    with patch(module_path, return_value=manager):
        adapter.search(influenced_query, 5)

    assert client.get.call_args.args[0] == expected_origin
    assert client.get.call_args.kwargs["params"]["q"] == influenced_query


def test_firecrawl_declared_ops_return_normalized_shapes():
    client = MagicMock()
    client.post.side_effect = [
        httpx.Response(
            200,
            json={
                "success": True,
                "data": {
                    "web": [
                        {
                            "title": "Search result",
                            "url": "https://result.example/search",
                            "description": "Snippet",
                            "markdown": "Search body",
                        }
                    ]
                },
            },
        ),
        httpx.Response(
            200,
            json={
                "success": True,
                "data": {
                    "markdown": "Extracted body",
                    "metadata": {"sourceURL": "https://result.example/extract"},
                },
            },
        ),
        httpx.Response(200, json={"success": True, "id": "crawl-job"}),
        httpx.Response(
            200,
            json={
                "success": True,
                "links": [
                    {"url": "https://result.example/one"},
                    "https://result.example/two",
                ],
            },
        ),
    ]
    client.get.return_value = httpx.Response(
        200,
        json={
            "status": "completed",
            "data": [
                {
                    "markdown": "Crawled body",
                    "metadata": {"sourceURL": "https://result.example/crawl"},
                }
            ],
        },
    )
    manager = MagicMock()
    manager.__enter__.return_value = client
    manager.__exit__.return_value = False
    adapter = FirecrawlAdapter(
        base_url="https://firecrawl.internal/v2",
        api_key="fc-test",
    )

    with patch(
        "agent.tools.research.search.firecrawl.httpx.Client", return_value=manager
    ):
        assert adapter.search("query", 5, include_raw_content=True) == [
            Result(
                title="Search result",
                url="https://result.example/search",
                snippet="Snippet",
                raw_content="Search body",
            )
        ]
        assert adapter.extract(["https://result.example/extract"]) == [
            Page(url="https://result.example/extract", content="Extracted body")
        ]
        assert adapter.crawl("https://result.example") == [
            Page(url="https://result.example/crawl", content="Crawled body")
        ]
        assert adapter.map("https://result.example") == [
            "https://result.example/one",
            "https://result.example/two",
        ]

    assert adapter.ops == frozenset({"search", "extract", "crawl", "map"})
    assert client.get.call_args.args[0] == (
        "https://firecrawl.internal/v2/crawl/crawl-job"
    )


@pytest.mark.parametrize("op", ["extract", "crawl", "map"])
def test_firecrawl_undeclared_ops_raise(op):
    adapter = FirecrawlAdapter(
        base_url="https://firecrawl.internal/v2",
        ops=frozenset({"search"}),
    )

    with pytest.raises(ProviderRequestError):
        if op == "extract":
            adapter.extract(["https://example.com"])
        else:
            getattr(adapter, op)("https://example.com")


def test_firecrawl_zero_results_is_an_answer():
    response = httpx.Response(
        200,
        json={"success": True, "data": {"web": []}},
    )
    manager, client = _http_client(response)
    client.post.return_value = response
    adapter = FirecrawlAdapter(base_url="https://firecrawl.internal/v2")

    with patch(
        "agent.tools.research.search.firecrawl.httpx.Client", return_value=manager
    ):
        assert adapter.search("nothing", 5) == []


def test_firecrawl_classifies_rate_limit():
    response = httpx.Response(429)
    manager, client = _http_client(response)
    client.post.return_value = response
    adapter = FirecrawlAdapter(base_url="https://firecrawl.internal/v2")

    with (
        patch(
            "agent.tools.research.search.firecrawl.httpx.Client", return_value=manager
        ),
        pytest.raises(ProviderRateLimitError),
    ):
        adapter.search("query", 5)


def test_firecrawl_cloud_requires_a_key_and_timeout_can_fail_over():
    with pytest.raises(ProviderAuthError):
        FirecrawlAdapter().search("query", 5)

    response = httpx.Response(408)
    manager, client = _http_client(response)
    client.post.return_value = response
    adapter = FirecrawlAdapter(api_key="fc-test")
    with (
        patch(
            "agent.tools.research.search.firecrawl.httpx.Client", return_value=manager
        ),
        pytest.raises(ProviderUnavailableError) as raised,
    ):
        adapter.search("query", 5)
    assert raised.value.status_code == 408


@pytest.mark.parametrize(
    ("method", "expected_endpoint"),
    [
        ("extract", "https://firecrawl.internal/v2/scrape"),
        ("crawl", "https://firecrawl.internal/v2/crawl"),
        ("map", "https://firecrawl.internal/v2/map"),
    ],
)
def test_firecrawl_target_url_never_changes_http_request_origin(
    method, expected_endpoint
):
    influenced_url = "http://169.254.169.254/latest/meta-data"
    client = MagicMock()
    if method == "extract":
        client.post.return_value = httpx.Response(
            200,
            json={
                "success": True,
                "data": {"markdown": "", "metadata": {"sourceURL": influenced_url}},
            },
        )
    elif method == "crawl":
        client.post.return_value = httpx.Response(
            200, json={"success": True, "id": "job-id"}
        )
        client.get.return_value = httpx.Response(
            200, json={"status": "completed", "data": []}
        )
    else:
        client.post.return_value = httpx.Response(
            200, json={"success": True, "links": []}
        )
    manager = MagicMock()
    manager.__enter__.return_value = client
    manager.__exit__.return_value = False
    adapter = FirecrawlAdapter(base_url="https://firecrawl.internal/v2")

    with patch(
        "agent.tools.research.search.firecrawl.httpx.Client", return_value=manager
    ):
        if method == "extract":
            adapter.extract([influenced_url])
        else:
            getattr(adapter, method)(influenced_url)

    assert client.post.call_args.args[0] == expected_endpoint
    assert client.post.call_args.kwargs["json"]["url"] == influenced_url


def test_firecrawl_is_registered_and_constructed():
    assert "firecrawl" in ADAPTER_NAMES
    adapter = create_search_adapter(
        {
            "provider": "firecrawl",
            "base_url": "https://firecrawl.internal/v2",
            "api_key": None,
            "ops": ["search", "extract", "crawl", "map"],
        }
    )

    assert isinstance(adapter, FirecrawlAdapter)
    assert adapter.ops == frozenset({"search", "extract", "crawl", "map"})


def test_firecrawl_accepts_api_root_with_or_without_v2():
    root = FirecrawlAdapter(base_url="https://firecrawl.internal")
    versioned = FirecrawlAdapter(base_url="https://firecrawl.internal/v2/")

    assert root.search_endpoint == "https://firecrawl.internal/v2/search"
    assert versioned.search_endpoint == "https://firecrawl.internal/v2/search"


def test_crawl4ai_declared_ops_return_normalized_shapes():
    client = MagicMock()
    client.post.side_effect = [
        httpx.Response(
            200,
            json={
                "success": True,
                "results": [
                    {
                        "url": "https://result.example/extract",
                        "success": True,
                        "markdown": {"raw_markdown": "Extracted body"},
                    },
                    {
                        "url": "https://result.example/blocked",
                        "success": False,
                        "error_message": "blocked by robots.txt",
                    },
                ],
            },
        ),
        httpx.Response(
            200,
            json={
                "success": True,
                "results": [
                    {
                        "url": "https://result.example/",
                        "success": True,
                        "markdown": {"raw_markdown": "Root body"},
                        "links": {
                            "internal": [
                                {"href": "https://result.example/docs"},
                                {"href": "https://result.example/api"},
                            ]
                        },
                    }
                ],
            },
        ),
        httpx.Response(
            200,
            json={
                "success": True,
                "results": [
                    {
                        "url": "https://result.example/docs",
                        "success": True,
                        "markdown": {"fit_markdown": "Docs body"},
                    }
                ],
            },
        ),
    ]
    manager = MagicMock()
    manager.__enter__.return_value = client
    manager.__exit__.return_value = False
    adapter = Crawl4AIAdapter(
        base_url="https://crawl4ai.internal/api",
        api_key="crawl4ai-test",
    )

    with patch(
        "agent.tools.research.search.crawl4ai.httpx.Client", return_value=manager
    ):
        assert adapter.extract(
            [
                "https://result.example/extract",
                "https://result.example/blocked",
            ]
        ) == [
            Page(url="https://result.example/extract", content="Extracted body"),
            Page(
                url="https://result.example/blocked",
                content="",
                failed="blocked by robots.txt",
            ),
        ]
        assert adapter.crawl(
            "https://result.example/",
            max_depth=1,
            max_breadth=1,
            limit=2,
        ) == [
            Page(url="https://result.example/", content="Root body"),
            Page(url="https://result.example/docs", content="Docs body"),
        ]

    assert adapter.ops == frozenset({"extract", "crawl"})
    assert all(
        call.args[0] == "https://crawl4ai.internal/api/crawl"
        for call in client.post.call_args_list
    )
    assert client.post.call_args_list[-1].kwargs["json"] == {
        "urls": ["https://result.example/docs"]
    }
    assert client.post.call_args_list[-1].kwargs["headers"]["Authorization"] == (
        "Bearer crawl4ai-test"
    )


@pytest.mark.parametrize("op", ["search", "map"])
def test_crawl4ai_undeclared_ops_raise(op):
    adapter = Crawl4AIAdapter(
        base_url="https://crawl4ai.internal",
        api_key="crawl4ai-test",
    )

    with pytest.raises(ProviderRequestError):
        if op == "search":
            adapter.search("query", 5)
        else:
            adapter.map("https://example.com")


@pytest.mark.parametrize("op", ["extract", "crawl"])
def test_crawl4ai_restricted_ops_raise(op):
    declared_op = "crawl" if op == "extract" else "extract"
    adapter = Crawl4AIAdapter(
        base_url="https://crawl4ai.internal",
        api_key="crawl4ai-test",
        ops=frozenset({declared_op}),
    )

    with pytest.raises(ProviderRequestError):
        if op == "extract":
            adapter.extract(["https://example.com"])
        else:
            adapter.crawl("https://example.com")


def test_crawl4ai_empty_results_is_answer_and_target_stays_payload():
    response = httpx.Response(200, json={"success": True, "results": []})
    manager, client = _http_client(response)
    client.post.return_value = response
    influenced_url = "http://169.254.169.254/latest/meta-data"
    adapter = Crawl4AIAdapter(
        base_url="https://crawl4ai.internal/root",
        api_key="crawl4ai-test",
    )

    with patch(
        "agent.tools.research.search.crawl4ai.httpx.Client", return_value=manager
    ) as client_class:
        assert adapter.extract([influenced_url]) == []

    client_class.assert_called_once_with(timeout=120.0, follow_redirects=False)
    assert client.post.call_args.args[0] == "https://crawl4ai.internal/root/crawl"
    assert client.post.call_args.kwargs["json"] == {"urls": [influenced_url]}


def test_crawl4ai_requires_token():
    with pytest.raises(ProviderAuthError):
        Crawl4AIAdapter(base_url="https://crawl4ai.internal").extract(
            ["https://example.com"]
        )


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        (401, ProviderAuthError),
        (408, ProviderUnavailableError),
        (429, ProviderRateLimitError),
        (500, ProviderUnavailableError),
    ],
)
def test_crawl4ai_classifies_provider_errors(status, expected):
    response = httpx.Response(status)
    manager, client = _http_client(response)
    client.post.return_value = response
    adapter = Crawl4AIAdapter(
        base_url="https://crawl4ai.internal",
        api_key="crawl4ai-test",
    )
    with (
        patch(
            "agent.tools.research.search.crawl4ai.httpx.Client",
            return_value=manager,
        ),
        pytest.raises(expected),
    ):
        adapter.extract(["https://example.com"])


def test_crawl4ai_is_registered_and_constructed_with_supported_ops_only():
    assert "crawl4ai" in ADAPTER_NAMES
    adapter = create_search_adapter(
        {
            "provider": "crawl4ai",
            "base_url": "https://crawl4ai.internal",
            "api_key": "crawl4ai-test",
            "ops": ["search", "extract", "crawl", "map"],
        }
    )

    assert isinstance(adapter, Crawl4AIAdapter)
    assert adapter.ops == frozenset({"extract", "crawl"})
