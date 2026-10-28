"""Provider-neutral web search and content retrieval tools."""

import asyncio
import logging
from typing import Any, Dict, List, Literal, Optional

from langchain_core.tools import tool

from agent.tools.context import ToolContext
from agent.tools.research.search import (
    ProviderError,
    SearchAdapter,
    create_search_adapter,
)

from shared.tool_catalog.definitions import (
    RESEARCH_TOOLS_METADATA as RESEARCH_TOOLS_METADATA,
)
from shared.untrusted_content import fence_untrusted

logger = logging.getLogger(__name__)

# Maximum words per result/page to protect LLM context window
MAX_RAW_CONTENT_WORDS = 5000
MAX_SNIPPET_CHARS = 1000
MAX_TOTAL_INLINE_CHARS = 60_000
NO_WORKSPACE_MAX_WORDS = 1500

# Tool metadata for registry
# Phase availability: domain tools are tactical-only


def _parse_comma_list(value: Optional[str]) -> Optional[List[str]]:
    """Parse a comma-separated string into a list, or return None."""
    if not value:
        return None
    items = [item.strip() for item in value.split(",") if item.strip()]
    return items if items else None


def _truncate_content(content: str, max_words: int = MAX_RAW_CONTENT_WORDS) -> str:
    """Truncate content to max_words, appending a note if truncated."""
    if not content:
        return ""
    words = content.split()
    if len(words) > max_words:
        return (
            " ".join(words[:max_words]) + f"\n... (truncated from {len(words)} words)"
        )
    return content


def _truncate_snippet(content: str, max_chars: int = MAX_SNIPPET_CHARS) -> str:
    """Truncate content to a compact character-bounded snippet."""
    if not content:
        return ""
    text = content.strip()
    if len(text) > max_chars:
        return text[:max_chars].rstrip() + "..."
    return text


def _bounded_inline_excerpt(
    content: str,
    remaining_chars: int,
    max_words: int = NO_WORKSPACE_MAX_WORDS,
) -> tuple[str, int]:
    """Return an inline excerpt bounded by words and shared char budget."""
    remaining_chars = max(0, remaining_chars)
    if not content:
        return "", remaining_chars
    if remaining_chars <= 0:
        return "[inline excerpt omitted: aggregate cap reached]", 0

    excerpt = _truncate_content(content, max_words=max_words)
    if len(excerpt) <= remaining_chars:
        return excerpt, remaining_chars - len(excerpt)

    cap_note = "\n... (inline aggregate cap reached)"
    available_chars = max(0, remaining_chars - len(cap_note))
    if available_chars == 0:
        return cap_note.strip(), 0
    return excerpt[:available_chars].rstrip() + cap_note, 0


def _fence_page(text: str) -> str:
    """Fence page-derived text for the model (N50): a page can carry a prompt
    injection like a mail body can. Braces stay: page text is often code or
    JSON, and the citation and workspace copies keep the raw content."""
    return fence_untrusted(text, origin="web", strip_braces=False)


def _saved_content_line(saved_path: str) -> str:
    """Format the saved-content pointer shown in tool results."""
    return (
        f"   Full text saved: {saved_path} — read it or "
        "extract_webpage(url) if you need the whole page.\n"
    )


def _run_async(coro: Any, loop: Optional[asyncio.AbstractEventLoop] = None) -> Any:
    """Drive an async coroutine from synchronous web-tool code.

    ``ToolContext.get_or_register_web_source`` is async (it does I/O on the
    shared ``srw_vector`` pool), but the web tools are sync and run in executor
    threads with no running loop. Scheduling the coroutine on the loop that
    created the tools (captured in ``create_web_tools``) preserves asyncpg
    connection-pool affinity. Mirrors the bridge in ``knowledge_tools.py``.
    """
    if loop is not None and loop.is_running():
        return asyncio.run_coroutine_threadsafe(coro, loop).result()
    return asyncio.run(coro)


def _research_adapter(
    context: Optional[ToolContext], capability: str
) -> SearchAdapter | None:
    """Resolve one configured adapter for direct helper calls and tool creation."""

    if context is None or not isinstance(context.config, dict):
        return None
    research = context.config.get("research")
    if not isinstance(research, dict):
        return None
    return create_search_adapter(research.get(capability))


def _provider_failure(
    label: str, adapter: SearchAdapter, error: ProviderError, subject: str
) -> str:
    """Render a provider failure so it can never read as an empty result set.

    A quota-exhausted, invalid-key or rate-limited call is a broken tool, not
    "no results": the model gets a distinct ``<label> failed:`` result carrying
    the provider's message, and the operator gets provider + status in the log.
    The provider adapters already redact their own API key from the message.
    """
    logger.warning(
        "%s provider %s failed (%s, status=%s) for %s: %s",
        label,
        getattr(adapter, "provider", "unknown"),
        type(error).__name__,
        error.status_code,
        subject,
        error,
    )
    return f"{label} failed: {error}"


def create_web_tools(context: ToolContext) -> List[Any]:
    """Create web search tools with injected context.

    Args:
        context: ToolContext with dependencies

    Returns:
        List of LangChain tool functions
    """

    # Capture the loop at tool-creation time (async graph setup) so the sync
    # tools below can drive ToolContext's async source registration on the
    # original loop, preserving asyncpg pool affinity (see _run_async).
    try:
        _creator_loop: Optional[asyncio.AbstractEventLoop] = asyncio.get_running_loop()
    except RuntimeError:
        _creator_loop = None

    search_adapter = _research_adapter(context, "search")
    search_fallback_adapter = _research_adapter(context, "search_fallback")
    fetch_adapter = _research_adapter(context, "fetch")

    @tool
    def web_search(
        query: str,
        max_results: int = 5,
        search_depth: Literal["basic", "advanced"] = "basic",
        topic: Literal["general", "news", "finance"] = "general",
        time_range: Optional[Literal["day", "week", "month", "year"]] = None,
        include_domains: Optional[str] = None,
        exclude_domains: Optional[str] = None,
        include_raw_content: bool = False,
    ) -> str:
        """Search the web for information.

        Each result is automatically registered as a citation source.
        Use cite_web() with the URL to create citations from these sources.

        Args:
            query: Search query
            max_results: Maximum results to return (1-20, default 5)
            search_depth: "basic" (fast, 1 credit) or "advanced" (better relevance, 2 credits)
            topic: "general" (default), "news" (recent events), or "finance" (financial data)
            time_range: Filter by recency: "day", "week", "month", "year", or None
            include_domains: Comma-separated domains to restrict search to
            exclude_domains: Comma-separated domains to exclude from results
            include_raw_content: If true, fetch and archive raw page content.
                The result still returns compact snippets plus saved-file
                pointers instead of inlining full page bodies.

        Returns:
            Search results with snippets, saved-file pointers, URLs, and source IDs
        """
        return _direct_web_search(
            query,
            max_results,
            context,
            search_depth=search_depth,
            topic=topic,
            time_range=time_range,
            include_domains=include_domains,
            exclude_domains=exclude_domains,
            include_raw_content=include_raw_content,
            creator_loop=_creator_loop,
            adapter=search_adapter,
            fallback_adapter=search_fallback_adapter,
        )

    @tool
    def extract_webpage(
        urls: str,
        query: Optional[str] = None,
        extract_depth: Literal["basic", "advanced"] = "basic",
    ) -> str:
        """Extract full content from one or more web pages.

        Retrieves the complete text content of web pages as clean markdown.
        Useful for reading articles, documentation, or any web content in full.
        Each extracted URL is automatically registered as a citation source and
        archived to the workspace. Inline output is bounded per call; results
        past the budget return a snippet plus the saved-file path to read.

        Args:
            urls: URL or comma-separated list of URLs to extract (max 20)
            query: Optional query to rank extracted content by relevance
            extract_depth: "basic" (fast, default) or "advanced" (JS-heavy sites)

        Returns:
            Extracted content (bounded) from each URL with source IDs for
            citation and saved-file pointers
        """
        return _extract_webpage(
            urls,
            context,
            query=query,
            extract_depth=extract_depth,
            creator_loop=_creator_loop,
            adapter=fetch_adapter,
        )

    @tool
    def crawl_website(
        url: str,
        instructions: Optional[str] = None,
        max_depth: int = 1,
        max_breadth: int = 20,
        limit: int = 20,
        select_paths: Optional[str] = None,
        exclude_paths: Optional[str] = None,
    ) -> str:
        """Crawl a website starting from a URL.

        Performs a breadth-first traversal from the starting URL, extracting
        and archiving content from discovered pages. Each crawled page is
        automatically registered as a citation source. Good for exploring
        documentation sites, regulatory pages, or multi-page resources.

        Args:
            url: Starting URL to crawl from
            instructions: Natural language guidance for the crawler
            max_depth: Link hops from start URL (1-5, default 1)
            max_breadth: Links to follow per page (1-500, default 20)
            limit: Total pages to crawl (default 20, keep low for cost)
            select_paths: Comma-separated regex patterns for paths to include
            exclude_paths: Comma-separated regex patterns for paths to exclude

        Returns:
            Compact crawled page snippets, saved-file pointers, and source IDs
            for citation
        """
        return _crawl_website(
            url,
            context,
            instructions=instructions,
            max_depth=max_depth,
            max_breadth=max_breadth,
            limit=limit,
            select_paths=select_paths,
            exclude_paths=exclude_paths,
            creator_loop=_creator_loop,
            adapter=fetch_adapter,
        )

    @tool
    def map_website(
        url: str,
        instructions: Optional[str] = None,
        max_depth: int = 2,
        limit: int = 50,
        select_paths: Optional[str] = None,
        exclude_paths: Optional[str] = None,
    ) -> str:
        """Map a website's structure to discover URLs.

        Creates a sitemap-like listing of all discoverable URLs on a website.
        Does NOT extract content - use extract_webpage or crawl_website for that.
        Useful for understanding site structure before targeted extraction.

        Args:
            url: Starting URL to map from
            instructions: Natural language guidance for URL discovery
            max_depth: Exploration depth (1-5, default 2)
            limit: Maximum URLs to discover (default 50)
            select_paths: Comma-separated regex patterns for paths to include
            exclude_paths: Comma-separated regex patterns for paths to exclude

        Returns:
            List of discovered URLs
        """
        return _map_website(
            url,
            instructions=instructions,
            max_depth=max_depth,
            limit=limit,
            select_paths=select_paths,
            exclude_paths=exclude_paths,
            context=context,
            adapter=fetch_adapter,
        )

    tools = []
    if search_adapter is not None and "search" in search_adapter.ops:
        tools.append(web_search)
    if fetch_adapter is not None:
        if "extract" in fetch_adapter.ops:
            tools.append(extract_webpage)
        if "crawl" in fetch_adapter.ops:
            tools.append(crawl_website)
        if "map" in fetch_adapter.ops:
            tools.append(map_website)
    return tools


def _direct_web_search(
    query: str,
    max_results: int,
    context: Optional[ToolContext] = None,
    search_depth: str = "basic",
    topic: str = "general",
    time_range: Optional[str] = None,
    include_domains: Optional[str] = None,
    exclude_domains: Optional[str] = None,
    include_raw_content: bool = False,
    creator_loop: Optional[asyncio.AbstractEventLoop] = None,
    adapter: SearchAdapter | None = None,
    fallback_adapter: SearchAdapter | None = None,
) -> str:
    """Search the web through the configured provider.

    Args:
        query: Search query
        max_results: Maximum results to return
        context: Optional ToolContext for source registration
        search_depth: "basic" or "advanced"
        topic: "general" or "news"
        time_range: "day", "week", "month", "year", or None
        include_domains: Comma-separated domains to include
        exclude_domains: Comma-separated domains to exclude
        include_raw_content: Whether to fetch and archive raw page content

    Returns:
        Search results with snippets, saved-file pointers, URLs, and source IDs
        (if context provided)
    """
    adapter = adapter or _research_adapter(context, "search")
    if adapter is None:
        return "Error: web search provider not configured"

    try:
        parsed_include = _parse_comma_list(include_domains)
        parsed_exclude = _parse_comma_list(exclude_domains)
        search_kwargs = {
            "search_depth": search_depth,
            "topic": topic,
            "time_range": time_range,
            "include_domains": parsed_include,
            "exclude_domains": parsed_exclude,
            "include_raw_content": include_raw_content,
        }
        fallback_annotation = ""
        try:
            provider_results = adapter.search(query, max_results, **search_kwargs)
        except ProviderError as primary_error:
            if not primary_error.failover_eligible or fallback_adapter is None:
                raise
            logger.warning(
                "Web search falling back from %s to %s after %s: %s",
                adapter.provider,
                fallback_adapter.provider,
                type(primary_error).__name__,
                primary_error,
            )
            try:
                provider_results = fallback_adapter.search(
                    query, max_results, **search_kwargs
                )
            except Exception:
                # The primary is the actionable operator signal. A failed
                # fallback must not replace it with a secondary symptom.
                raise primary_error
            fallback_annotation = (
                f"Provider fallback: {fallback_adapter.provider} answered after "
                f"{adapter.provider} failed.\n"
            )
        results = [
            {
                "title": item.title,
                "url": item.url,
                "content": item.snippet,
                "raw_content": item.raw_content,
            }
            for item in provider_results
        ]
        if not results:
            return fallback_annotation + f"No web results found for: {query}"

        # Register each result as a citation source if context available
        registered_sources = []
        inaccessible_sources = []
        if context is not None:
            for r in results:
                url = r.get("url", "")
                title = r.get("title", "Untitled")
                if url:
                    try:
                        provider_content = (
                            r.get("raw_content") or r.get("content") or title
                        )
                        source_id, fetch_error = _run_async(
                            context.get_or_register_web_source(
                                url,
                                name=title,
                                content=provider_content,
                            ),
                            creator_loop,
                        )
                        registered_sources.append((url, source_id))
                        if fetch_error:
                            inaccessible_sources.append((url, source_id))
                    except Exception as e:
                        logger.warning(f"Could not register web source {url}: {e}")

        # Save web content to disk for persistence
        saved_paths: Dict[str, str] = {}
        if context is not None:
            for r in results:
                url = r.get("url", "")
                if url:
                    raw_content = r.get("raw_content")
                    content = raw_content or r.get("content") or ""
                    if content and (raw_content or len(content) > 50):
                        title = r.get("title", "Untitled")
                        source_id = next(
                            (sid for u, sid in registered_sources if u == url), None
                        )
                        saved_path = context.save_web_content_to_disk(
                            url, content, title=title, source_id=source_id
                        )
                        if saved_path:
                            saved_paths[url] = saved_path

        # Format output
        result = fallback_annotation + f"Web Search Results for: {query}\n"
        if registered_sources:
            result += f"Results: {len(results)} ({len(registered_sources)} archived as citation sources)\n\n"
        else:
            result += f"Results: {len(results)}\n\n"

        remaining_fallback_chars = MAX_TOTAL_INLINE_CHARS
        for i, r in enumerate(results, 1):
            url = r.get("url", "N/A")
            source_id = next((sid for u, sid in registered_sources if u == url), None)
            is_inaccessible = any(u == url for u, _ in inaccessible_sources)
            saved_path = saved_paths.get(url)
            raw_content = r.get("raw_content") or r.get("content") or ""
            snippet = r.get("content") or raw_content
            result += f"{i}. {r.get('title', 'Untitled')}\n"
            result += f"   URL: {url}\n"
            if source_id and is_inaccessible:
                result += (
                    f"   Source ID: {source_id} (INACCESSIBLE - content not fetched)\n"
                )
            elif source_id:
                result += f"   Source ID: {source_id} (archived)\n"

            result += f"   Snippet:\n{_fence_page(_truncate_snippet(snippet))}\n"
            if saved_path and not is_inaccessible:
                result += _saved_content_line(saved_path)
            elif (
                not saved_path
                and not is_inaccessible
                and include_raw_content
                and raw_content
            ):
                excerpt, remaining_fallback_chars = _bounded_inline_excerpt(
                    raw_content,
                    remaining_fallback_chars,
                    max_words=NO_WORKSPACE_MAX_WORDS,
                )
                result += f"   Content excerpt (not saved):\n{_fence_page(excerpt)}\n"
            result += "\n"

        if inaccessible_sources:
            result += (
                f"\nWARNING: {len(inaccessible_sources)} source(s) could not be fetched automatically "
                f"(HTTP 403 or similar). Use the browser tool to manually download content from these URLs "
                f"if you need to cite them:\n"
            )
            for url, _ in inaccessible_sources:
                result += f"  - {url}\n"
            result += "\n"

        if registered_sources:
            result += "To cite: use cite_web(text, url) - sources are already archived."
        else:
            result += "To cite information from these results, use cite_web(text, url, title) for each source you reference."

        return result

    except ProviderError as e:
        return _provider_failure("Web search", adapter, e, f"query {query!r}")
    except Exception as e:
        logger.exception("Web search failed for query: %s", query)
        return f"Error searching web: {str(e)}"


def _extract_webpage(
    urls: str,
    context: Optional[ToolContext] = None,
    query: Optional[str] = None,
    extract_depth: str = "basic",
    creator_loop: Optional[asyncio.AbstractEventLoop] = None,
    adapter: SearchAdapter | None = None,
) -> str:
    """Extract full content from web pages through the configured provider.

    Args:
        urls: Comma-separated URLs to extract
        context: Optional ToolContext for source registration
        query: Optional relevance ranking query
        extract_depth: "basic" or "advanced"

    Returns:
        Extracted content from each URL
    """
    url_list = [u.strip() for u in urls.split(",") if u.strip()]
    if not url_list:
        return "Error: No URLs provided"
    if len(url_list) > 20:
        return "Error: Maximum 20 URLs allowed per request"

    adapter = adapter or _research_adapter(context, "fetch")
    if adapter is None:
        return "Error: web fetch provider not configured"

    try:
        pages = adapter.extract(
            url_list,
            query=query,
            extract_depth=extract_depth,
        )
        results = [
            {"url": page.url, "raw_content": page.content}
            for page in pages
            if page.failed is None
        ]
        failed = [page.url for page in pages if page.failed is not None]

        if not results and not failed:
            return "No content could be extracted from the provided URL(s)."

        # Register extracted URLs as citation sources
        registered = []
        if context is not None:
            for r in results:
                url = r.get("url", "")
                if url:
                    try:
                        provider_content = r.get("raw_content") or (
                            f"Provider returned no page content for {url}"
                        )
                        source_id, fetch_error = _run_async(
                            context.get_or_register_web_source(
                                url,
                                content=provider_content,
                            ),
                            creator_loop,
                        )
                        registered.append((url, source_id))
                    except Exception as e:
                        logger.warning(f"Could not register source {url}: {e}")

        # Save web content to disk for persistence (before truncation)
        saved_paths: Dict[str, str] = {}
        if context is not None:
            for r in results:
                url = r.get("url", "")
                raw = r.get("raw_content", "")
                if url and raw:
                    source_id = next((sid for u, sid in registered if u == url), None)
                    saved_path = context.save_web_content_to_disk(
                        url, raw, source_id=source_id
                    )
                    if saved_path:
                        saved_paths[url] = saved_path

        # Format output
        output = f"Extracted Content from {len(results)} URL(s)"
        if failed:
            output += f" ({len(failed)} failed)"
        output += ":\n\n"

        remaining_inline_chars = MAX_TOTAL_INLINE_CHARS
        for i, r in enumerate(results, 1):
            url = r.get("url", "N/A")
            raw_content = r.get("raw_content") or ""
            source_id = next((sid for u, sid in registered if u == url), None)
            saved_path = saved_paths.get(url)

            word_count = len(raw_content.split()) if raw_content else 0

            output += f"{i}. {url}\n"
            if source_id:
                output += f"   Source ID: {source_id} (archived)\n"
            output += f"   Words: {word_count:,}\n"

            inline_content = _truncate_content(raw_content)
            if saved_path and len(inline_content) <= remaining_inline_chars:
                remaining_inline_chars -= len(inline_content)
                output += f"   Content:\n{_fence_page(inline_content)}\n\n"
            elif saved_path:
                remaining_inline_chars = 0
                output += (
                    f"   Snippet:\n{_fence_page(_truncate_snippet(raw_content))}\n"
                )
                output += _saved_content_line(saved_path)
                output += "\n"
            else:
                excerpt, remaining_inline_chars = _bounded_inline_excerpt(
                    raw_content,
                    remaining_inline_chars,
                    max_words=NO_WORKSPACE_MAX_WORDS,
                )
                output += f"   Content excerpt (not saved):\n{_fence_page(excerpt)}\n\n"

        if failed:
            output += "Failed URLs:\n"
            for f_url in failed:
                u = f_url if isinstance(f_url, str) else f_url.get("url", str(f_url))
                output += f"  - {u}\n"

        if registered:
            output += (
                "\nTo cite: use cite_web(text, url) - sources are already archived."
            )

        return output

    except ProviderError as e:
        return _provider_failure("Webpage extraction", adapter, e, f"URLs {urls}")
    except Exception as e:
        logger.exception("Web extract failed for URLs: %s", urls)
        return f"Error extracting content: {str(e)}"


def _crawl_website(
    url: str,
    context: Optional[ToolContext] = None,
    instructions: Optional[str] = None,
    max_depth: int = 1,
    max_breadth: int = 20,
    limit: int = 20,
    select_paths: Optional[str] = None,
    exclude_paths: Optional[str] = None,
    creator_loop: Optional[asyncio.AbstractEventLoop] = None,
    adapter: SearchAdapter | None = None,
) -> str:
    """Crawl a website through the configured provider.

    Args:
        url: Starting URL
        context: Optional ToolContext for source registration
        instructions: Natural language guidance
        max_depth: Link hops (1-5)
        max_breadth: Links per page (1-500)
        limit: Total pages
        select_paths: Comma-separated path regex patterns to include
        exclude_paths: Comma-separated path regex patterns to exclude

    Returns:
        Crawled content from each page
    """
    # Clamp parameters
    max_depth = max(1, min(5, max_depth))
    max_breadth = max(1, min(500, max_breadth))
    limit = max(1, limit)

    adapter = adapter or _research_adapter(context, "fetch")
    if adapter is None:
        return "Error: web fetch provider not configured"

    try:
        parsed_select = _parse_comma_list(select_paths)
        parsed_exclude = _parse_comma_list(exclude_paths)
        pages = adapter.crawl(
            url,
            instructions=instructions,
            max_depth=max_depth,
            max_breadth=max_breadth,
            limit=limit,
            select_paths=parsed_select,
            exclude_paths=parsed_exclude,
        )
        results = [
            {"url": page.url, "raw_content": page.content}
            for page in pages
            if page.failed is None
        ]

        if not results:
            return f"No pages could be crawled from: {url}"

        # Register crawled URLs as citation sources
        registered = []
        if context is not None:
            for r in results:
                page_url = r.get("url", "")
                if page_url:
                    try:
                        provider_content = r.get("raw_content") or (
                            f"Provider returned no page content for {page_url}"
                        )
                        source_id, fetch_error = _run_async(
                            context.get_or_register_web_source(
                                page_url,
                                content=provider_content,
                            ),
                            creator_loop,
                        )
                        registered.append((page_url, source_id))
                    except Exception as e:
                        logger.warning(f"Could not register source {page_url}: {e}")

        # Save web content to disk for persistence (before truncation)
        saved_paths: Dict[str, str] = {}
        if context is not None:
            for r in results:
                page_url = r.get("url", "")
                raw = r.get("raw_content", "")
                if page_url and raw:
                    source_id = next(
                        (sid for u, sid in registered if u == page_url), None
                    )
                    saved_path = context.save_web_content_to_disk(
                        page_url, raw, source_id=source_id
                    )
                    if saved_path:
                        saved_paths[page_url] = saved_path

        # Format output
        output = f"Website Crawl Results for: {url}\n"
        output += f"Pages crawled: {len(results)}\n\n"

        remaining_fallback_chars = MAX_TOTAL_INLINE_CHARS
        for i, r in enumerate(results, 1):
            page_url = r.get("url", "N/A")
            raw_content = r.get("raw_content") or ""
            source_id = next((sid for u, sid in registered if u == page_url), None)
            saved_path = saved_paths.get(page_url)

            word_count = len(raw_content.split()) if raw_content else 0

            output += f"{i}. {page_url}\n"
            if source_id:
                output += f"   Source ID: {source_id} (archived)\n"
            output += f"   Words: {word_count:,}\n"
            if saved_path:
                output += (
                    f"   Snippet:\n{_fence_page(_truncate_snippet(raw_content, 500))}\n"
                )
                output += _saved_content_line(saved_path)
                output += "\n"
            else:
                excerpt, remaining_fallback_chars = _bounded_inline_excerpt(
                    raw_content,
                    remaining_fallback_chars,
                    max_words=NO_WORKSPACE_MAX_WORDS,
                )
                output += f"   Content excerpt (not saved):\n{_fence_page(excerpt)}\n\n"

        if registered:
            output += "To cite: use cite_web(text, url) - sources are already archived."

        return output

    except ProviderError as e:
        return _provider_failure("Website crawl", adapter, e, f"URL {url}")
    except Exception as e:
        logger.exception("Web crawl failed for URL: %s", url)
        return f"Error crawling website: {str(e)}"


def _map_website(
    url: str,
    instructions: Optional[str] = None,
    max_depth: int = 2,
    limit: int = 50,
    select_paths: Optional[str] = None,
    exclude_paths: Optional[str] = None,
    context: Optional[ToolContext] = None,
    adapter: SearchAdapter | None = None,
) -> str:
    """Map website structure through the configured provider.

    Args:
        url: Starting URL
        instructions: Natural language guidance
        max_depth: Exploration depth (1-5)
        limit: Maximum URLs
        select_paths: Comma-separated path regex patterns to include
        exclude_paths: Comma-separated path regex patterns to exclude

    Returns:
        List of discovered URLs
    """
    max_depth = max(1, min(5, max_depth))
    limit = max(1, limit)

    adapter = adapter or _research_adapter(context, "fetch")
    if adapter is None:
        return "Error: web fetch provider not configured"

    try:
        parsed_select = _parse_comma_list(select_paths)
        parsed_exclude = _parse_comma_list(exclude_paths)
        results = adapter.map(
            url,
            instructions=instructions,
            max_depth=max_depth,
            limit=limit,
            select_paths=parsed_select,
            exclude_paths=parsed_exclude,
        )

        if not results:
            return f"No URLs discovered for: {url}"

        output = f"Website Map for: {url}\n"
        output += f"URLs discovered: {len(results)}\n\n"

        for i, discovered_url in enumerate(results, 1):
            if isinstance(discovered_url, dict):
                discovered_url = discovered_url.get("url", str(discovered_url))
            output += f"{i}. {discovered_url}\n"

        output += "\nUse extract_webpage(urls) to read specific pages, "
        output += "or crawl_website(url) to crawl with content extraction."

        return output

    except ProviderError as e:
        return _provider_failure("Website map", adapter, e, f"URL {url}")
    except Exception as e:
        return f"Error mapping website: {str(e)}"
