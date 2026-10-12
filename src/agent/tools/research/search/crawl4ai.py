"""Crawl4AI 0.9.x fixed-destination remote-fetch adapter."""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any
from urllib.parse import urldefrag, urljoin, urlsplit

import httpx

from agent.tools.research.search.base import Page, Result
from agent.tools.research.search.errors import (
    ProviderAuthError,
    ProviderError,
    ProviderRequestError,
    ProviderUnavailableError,
)
from agent.tools.research.search.http import (
    configured_endpoint,
    raise_for_provider_status,
    transport_error,
)


_CRAWL4AI_OPS = frozenset({"extract", "crawl", "map"})
_MAX_REQUEST_URLS = 100
_MAX_CRAWL_PAGES = 100
_MAX_MAP_URLS = 500
# A map renders pages only to read their links, so it walks the frontier in
# small batches and stops once it has enough URLs instead of rendering a whole
# depth level it will not use.
_MAP_FETCH_BATCH = 10
_MAX_MAP_FETCHES = 50


def _path_patterns(value: Any, name: str) -> list[re.Pattern[str]]:
    """Compile model-supplied path regexes, rejecting a bad one as a bad request."""
    if not value:
        return []
    patterns = [value] if isinstance(value, str) else list(value)
    compiled: list[re.Pattern[str]] = []
    for pattern in patterns:
        try:
            compiled.append(re.compile(str(pattern)))
        except re.error as exc:
            raise ProviderRequestError(
                f"Invalid {name} pattern {pattern!r}: {exc}"
            ) from exc
    return compiled


class Crawl4AIAdapter:
    """Fetch through an admin-configured Crawl4AI Docker API service."""

    provider = "crawl4ai"
    supported_ops = _CRAWL4AI_OPS

    def __init__(
        self,
        *,
        base_url: str | None,
        api_key: str | None = None,
        ops: frozenset[str] | None = None,
    ) -> None:
        self.endpoint = configured_endpoint(base_url, "crawl")
        self.api_key = api_key.strip() if isinstance(api_key, str) else None
        requested_ops = ops if ops is not None else self.supported_ops
        self.ops = frozenset(requested_ops & self.supported_ops)

    def _require(self, op: str) -> None:
        if op not in self.ops or op not in self.supported_ops:
            raise ProviderRequestError(f"Crawl4AI does not support {op}")
        # Crawl4AI 0.9.x refuses a non-loopback bind without a credential, and
        # accepts the configured static token (or a minted JWT) as Bearer auth.
        if not self.api_key:
            raise ProviderAuthError("Crawl4AI API token is not configured")

    def _headers(self) -> dict[str, str]:
        return {
            "Accept": "application/json",
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

    @staticmethod
    def _payload(response: httpx.Response) -> list[Mapping[str, Any]]:
        if response.status_code == 408:
            raise ProviderUnavailableError(
                "Crawl4AI request timed out", status_code=408
            )
        raise_for_provider_status("Crawl4AI", response)
        payload = response.json()
        if not isinstance(payload, Mapping):
            raise ProviderUnavailableError("Crawl4AI returned an invalid response")
        if payload.get("success") is False:
            message = str(payload.get("error") or "Crawl4AI request failed")
            raise ProviderUnavailableError(message)
        items = payload.get("results")
        if not isinstance(items, list) or any(
            not isinstance(item, Mapping) for item in items
        ):
            raise ProviderUnavailableError("Crawl4AI returned invalid crawl data")
        return items

    @staticmethod
    def _content(item: Mapping[str, Any]) -> str:
        markdown = item.get("markdown")
        if isinstance(markdown, Mapping):
            for key in (
                "raw_markdown",
                "fit_markdown",
                "markdown_with_citations",
            ):
                value = markdown.get(key)
                if value is not None:
                    return str(value)
        elif markdown is not None:
            return str(markdown)

        for key in ("extracted_content", "cleaned_html", "html"):
            value = item.get(key)
            if value is not None:
                return str(value)
        return ""

    @classmethod
    def _page(cls, item: Mapping[str, Any], fallback_url: str = "") -> Page:
        url = str(item.get("url") or fallback_url)
        failed: str | None = None
        if item.get("success") is False:
            failed = str(item.get("error_message") or "crawl failed")
        else:
            try:
                status = item.get("status_code")
                if status is not None and int(status) >= 400:
                    failed = str(item.get("error_message") or f"HTTP {int(status)}")
            except (TypeError, ValueError):
                pass
        return Page(
            url=url,
            content="" if failed is not None else cls._content(item),
            failed=failed,
        )

    @staticmethod
    def _internal_links(item: Mapping[str, Any], page_url: str) -> list[str]:
        links = item.get("links")
        internal = links.get("internal", []) if isinstance(links, Mapping) else []
        if not isinstance(internal, list):
            return []

        normalized: list[str] = []
        for entry in internal:
            href = entry.get("href") if isinstance(entry, Mapping) else entry
            if not isinstance(href, str) or not href.strip():
                continue
            candidate, _fragment = urldefrag(urljoin(page_url, href.strip()))
            parsed = urlsplit(candidate)
            if parsed.scheme in {"http", "https"} and parsed.hostname:
                normalized.append(candidate)
        return normalized

    def _fetch(
        self,
        client: httpx.Client,
        urls: list[str],
    ) -> list[tuple[Page, list[str]]]:
        fetched: list[tuple[Page, list[str]]] = []
        for offset in range(0, len(urls), _MAX_REQUEST_URLS):
            batch = urls[offset : offset + _MAX_REQUEST_URLS]
            response = client.post(
                self.endpoint,
                json={"urls": batch},
                headers=self._headers(),
            )
            items = self._payload(response)
            for index, item in enumerate(items):
                fallback_url = batch[index] if index < len(batch) else ""
                page = self._page(item, fallback_url)
                fetched.append((page, self._internal_links(item, page.url)))
        return fetched

    def extract(self, urls: list[str], **kw: Any) -> list[Page]:
        self._require("extract")
        del kw
        if not urls:
            return []
        try:
            with httpx.Client(timeout=120.0, follow_redirects=False) as client:
                return [page for page, _links in self._fetch(client, urls)]
        except ProviderError:
            raise
        except Exception as exc:
            raise transport_error("Crawl4AI", exc) from exc

    def crawl(self, url: str, **kw: Any) -> list[Page]:
        self._require("crawl")
        try:
            max_depth = min(max(int(kw.get("max_depth", 1)), 1), 5)
            max_breadth = min(max(int(kw.get("max_breadth", 20)), 1), 500)
            limit = min(max(int(kw.get("limit", 20)), 1), _MAX_CRAWL_PAGES)

            pages: list[Page] = []
            frontier = [url]
            seen = {urldefrag(url)[0]}
            with httpx.Client(timeout=120.0, follow_redirects=False) as client:
                # Crawl4AI 0.9.x rejects request-supplied deep-crawl strategy
                # objects. Traverse its returned internal links here while every
                # network request remains pinned to the configured API endpoint.
                for depth in range(max_depth + 1):
                    if not frontier or len(pages) >= limit:
                        break
                    fetched = self._fetch(client, frontier[: limit - len(pages)])
                    pages.extend(page for page, _links in fetched)
                    pages = pages[:limit]
                    if depth >= max_depth or len(pages) >= limit:
                        break

                    next_frontier: list[str] = []
                    for _page, links in fetched:
                        added_for_page = 0
                        for candidate in links:
                            if candidate in seen:
                                continue
                            seen.add(candidate)
                            next_frontier.append(candidate)
                            added_for_page += 1
                            if added_for_page >= max_breadth:
                                break
                            if len(pages) + len(next_frontier) >= limit:
                                break
                        if len(pages) + len(next_frontier) >= limit:
                            break
                    frontier = next_frontier
            return pages
        except ProviderError:
            raise
        except Exception as exc:
            raise transport_error("Crawl4AI", exc) from exc

    def search(self, query: str, max_results: int, **kw: Any) -> list[Result]:
        del query, max_results, kw
        raise ProviderRequestError("Crawl4AI does not support search")

    def map(self, url: str, **kw: Any) -> list[str]:
        """Discover a site's URLs by following internal links breadth-first.

        Crawl4AI 0.9.x exposes no URL-discovery endpoint (its URL seeder is
        library-only), so this walks the same returned internal links as
        ``crawl`` but keeps only the URLs. ``max_depth`` counts link hops from
        ``url``: depth 1 renders only the start page. ``select_paths`` /
        ``exclude_paths`` are regexes searched against the URL path and scope
        both what is listed and what is followed. ``instructions`` has no
        Crawl4AI equivalent and is ignored.
        """
        self._require("map")
        select = _path_patterns(kw.get("select_paths"), "select_paths")
        exclude = _path_patterns(kw.get("exclude_paths"), "exclude_paths")

        def wanted(candidate: str) -> bool:
            path = urlsplit(candidate).path or "/"
            if select and not any(p.search(path) for p in select):
                return False
            return not any(p.search(path) for p in exclude)

        try:
            max_depth = min(max(int(kw.get("max_depth", 1)), 1), 5)
            limit = min(max(int(kw.get("limit", 50)), 1), _MAX_MAP_URLS)

            root = urldefrag(url)[0]
            discovered = [root] if wanted(root) else []
            seen = {root}
            frontier = [root]
            fetches = 0
            with httpx.Client(timeout=120.0, follow_redirects=False) as client:
                for _depth in range(max_depth):
                    next_frontier: list[str] = []
                    for offset in range(0, len(frontier), _MAP_FETCH_BATCH):
                        if len(discovered) >= limit or fetches >= _MAX_MAP_FETCHES:
                            break
                        batch = frontier[offset : offset + _MAP_FETCH_BATCH]
                        batch = batch[: _MAX_MAP_FETCHES - fetches]
                        fetches += len(batch)
                        for _page, links in self._fetch(client, batch):
                            for candidate in links:
                                if candidate in seen:
                                    continue
                                seen.add(candidate)
                                if not wanted(candidate):
                                    continue
                                discovered.append(candidate)
                                next_frontier.append(candidate)
                    frontier = next_frontier
                    if (
                        not frontier
                        or len(discovered) >= limit
                        or fetches >= _MAX_MAP_FETCHES
                    ):
                        break
            return discovered[:limit]
        except ProviderError:
            raise
        except Exception as exc:
            raise transport_error("Crawl4AI", exc) from exc
