"""The citation engine fetches public pages only (audit finding N34).

``cite_web`` without provider content makes the agent pod download the URL
and archive the body, which ``search_library`` then reads back. Every hop,
the first URL and each redirect, must be a public http(s) address; a
refusal surfaces to the model as an error and registers nothing. DNS
(``socket.getaddrinfo``) and HTTP (``requests.get``) are faked: no network.
"""

from __future__ import annotations

import io
import socket
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
import requests

from agent.citation_engine import CitationEngine
from agent.citation_engine.engine import MAX_WEB_FETCH_REDIRECTS
from agent.tools.citation.sources import create_source_tools
from agent.tools.context import ToolContext
from shared.url_safety import UnsafeURLError

DNS = {
    "public.example": "93.184.216.34",
    "other.example": "93.184.216.35",
    "rebound.example": "10.0.0.9",
}


@pytest.fixture(autouse=True)
def fake_dns(monkeypatch):
    def getaddrinfo(host, port, *args, **kwargs):
        try:
            address = DNS[host]
        except KeyError:
            raise socket.gaierror(socket.EAI_NONAME, "Name or service not known")
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (address, port))]

    monkeypatch.setattr(socket, "getaddrinfo", getaddrinfo)


def _response(url, status=200, body=b"", headers=None):
    response = requests.Response()
    response.url = url
    response.status_code = status
    response._content = body
    response.raw = io.BytesIO(body)
    response.headers.update(headers or {})
    return response


def _html(url, text="Archived page text", title="Page"):
    body = f"<html><head><title>{title}</title></head><body><p>{text}</p></body></html>"
    return _response(
        url, body=body.encode(), headers={"Content-Type": "text/html; charset=utf-8"}
    )


@pytest.fixture
def http(monkeypatch):
    """``Session.send`` answering from a {url: response} table, recording calls.

    The transport is the last step: what it receives is exactly what would
    go on the wire, so ``calls`` holds the URLs the pod would have dialled.
    """
    routes: dict[str, requests.Response] = {}
    calls: list[tuple[str, dict]] = []

    def send(self, prepared, **kwargs):
        calls.append((prepared.url, kwargs))
        if prepared.url not in routes:
            raise AssertionError(f"unexpected request to {prepared.url}")
        return routes[prepared.url]

    monkeypatch.setattr(requests.Session, "send", send)
    return SimpleNamespace(routes=routes, calls=calls)


@pytest.fixture
def engine():
    engine = CitationEngine(db=MagicMock())
    engine._register_source = AsyncMock(
        side_effect=lambda **kwargs: SimpleNamespace(id=1, **kwargs)
    )
    return engine


class TestPublicFetch:
    @pytest.mark.asyncio
    async def test_public_page_is_archived(self, engine, http):
        http.routes["https://public.example/a"] = _html("https://public.example/a")

        source = await engine.add_web_source("https://public.example/a")

        assert "Archived page text" in source.content
        assert source.metadata["title"] == "Page"
        assert "fetch_error" not in source.metadata
        url, kwargs = http.calls[0]
        assert url == "https://public.example/a"
        assert kwargs["allow_redirects"] is False

    @pytest.mark.asyncio
    async def test_redirects_between_public_hosts_are_followed(self, engine, http):
        http.routes["https://public.example/a"] = _response(
            "https://public.example/a", 301, headers={"Location": "/b"}
        )
        http.routes["https://public.example/b"] = _response(
            "https://public.example/b",
            302,
            headers={"Location": "https://other.example/final"},
        )
        http.routes["https://other.example/final"] = _html(
            "https://other.example/final"
        )

        source = await engine.add_web_source("https://public.example/a")

        assert "Archived page text" in source.content
        assert [url for url, _ in http.calls] == [
            "https://public.example/a",
            "https://public.example/b",
            "https://other.example/final",
        ]


class TestRefusedFetch:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "url",
        [
            "http://169.254.169.254/latest/meta-data/",
            "http://127.0.0.1:8080/health",
            "http://10.43.0.10:8085/api/jobs",
            "http://100.64.0.7/",
            "http://[::1]/",
            "http://[::ffff:10.0.0.1]/",
            "http://2852039166/",
            "http://rebound.example/",
            # requests dials 10.0.0.1 for both; a naive urlsplit sees
            # public.example and "%31%30.0.0.1".
            "http://10.0.0.1\\@public.example/",
            "http://%31%30.0.0.1/",
            "file:///etc/passwd",
            "gopher://public.example/_x",
        ],
    )
    async def test_refused_url_is_never_requested_or_registered(
        self, engine, http, url
    ):
        with pytest.raises(UnsafeURLError, match=r"^refused: "):
            await engine.add_web_source(url)

        assert http.calls == []
        engine._register_source.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "location",
        [
            "http://169.254.169.254/latest/meta-data/iam/",
            "http://rebound.example/internal",
            "http://[::ffff:127.0.0.1]:5432/",
            "file:///etc/passwd",
        ],
    )
    async def test_redirect_from_public_to_private_is_refused(
        self, engine, http, location
    ):
        http.routes["https://public.example/a"] = _response(
            "https://public.example/a", 302, headers={"Location": location}
        )

        with pytest.raises(UnsafeURLError, match=r"^refused: "):
            await engine.add_web_source("https://public.example/a")

        # The public hop was requested; the private target never was.
        assert [url for url, _ in http.calls] == ["https://public.example/a"]
        engine._register_source.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_redirect_cap(self, engine, http):
        hops = MAX_WEB_FETCH_REDIRECTS + 1
        for n in range(hops + 1):
            http.routes[f"https://public.example/{n}"] = _response(
                f"https://public.example/{n}",
                302,
                headers={"Location": f"/{n + 1}"},
            )

        source = await engine.add_web_source("https://public.example/0")

        # Too many redirects is an ordinary fetch failure: the existing
        # placeholder-and-fetch_error shape, not a refusal.
        assert len(http.calls) == hops
        assert "redirects" in source.metadata["fetch_error"]
        assert source.content.startswith("[CONTENT NOT ACCESSIBLE]")

    @pytest.mark.asyncio
    async def test_missing_scheme_keeps_the_fetch_failure_shape(self, engine, http):
        source = await engine.add_web_source("public.example/page")

        assert http.calls == []
        assert source.content.startswith("[CONTENT NOT ACCESSIBLE]")

    @pytest.mark.asyncio
    async def test_unresolvable_host_keeps_the_fetch_failure_shape(self, engine, http):
        source = await engine.add_web_source("https://nowhere.example/")

        assert http.calls == []
        assert source.metadata["fetch_error"]
        assert source.content.startswith("[CONTENT NOT ACCESSIBLE]")


class TestCiteWebSurfacesTheRefusal:
    @pytest.mark.asyncio
    async def test_model_sees_an_error_not_a_citation(self, engine, http):
        engine.cite_web = AsyncMock(
            side_effect=AssertionError("no citation for a refused URL")
        )
        context = ToolContext(citation_engine=engine)
        tools = {tool.name: tool for tool in create_source_tools(context)}

        result = await tools["cite_web"].ainvoke(
            {
                "text": "secret",
                "url": "http://169.254.169.254/latest/meta-data/",
            }
        )

        assert result.startswith(
            "Error: could not register web source "
            "'http://169.254.169.254/latest/meta-data/': refused: 169.254.169.254"
        )
        assert http.calls == []
        engine._register_source.assert_not_awaited()
