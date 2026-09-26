"""``GET {base_url}/models`` probe error reporting."""

import httpx
import pytest

from orchestrator.services import llm_endpoint_probe
from orchestrator.services.llm_endpoint_probe import probe_endpoint_models

SEARXNG_404 = (
    '<!DOCTYPE html>\n<html class="no-js theme-auto" lang="en-EN" >\n<head>\n'
    '  <meta name="description" content="SearXNG">\n</head><body></body></html>'
)


@pytest.fixture
def serve(monkeypatch):
    """Route the probe's client through a canned response."""

    def install(response: httpx.Response) -> None:
        real_client = httpx.AsyncClient

        def client(**kwargs):
            return real_client(
                transport=httpx.MockTransport(lambda request: response), **kwargs
            )

        monkeypatch.setattr(llm_endpoint_probe.httpx, "AsyncClient", client)

    return install


@pytest.mark.asyncio
async def test_html_error_page_is_summarized_not_echoed(serve):
    serve(
        httpx.Response(
            404,
            text=SEARXNG_404,
            headers={"content-type": "text/html; charset=utf-8"},
        )
    )

    result = await probe_endpoint_models("http://srw-searxng:8080", None)

    assert result.ok is False
    assert result.status == 404
    assert result.error == (
        "HTTP 404: http://srw-searxng:8080/models answered with a web page, "
        "not an OpenAI-compatible model list."
    )


@pytest.mark.asyncio
async def test_html_success_page_is_summarized_not_a_json_parse_error(serve):
    # No content type: the markup alone identifies the page.
    serve(httpx.Response(200, content=SEARXNG_404.encode()))

    result = await probe_endpoint_models("https://ui.invalid", None)

    assert result.ok is False
    assert result.status == 200
    assert "answered with a web page" in (result.error or "")


@pytest.mark.asyncio
async def test_json_error_body_stays_verbatim(serve):
    serve(
        httpx.Response(
            401,
            content=b'{"error":{"message":"Invalid API key"}}',
            headers={"content-type": "application/json"},
        )
    )

    result = await probe_endpoint_models("https://models.invalid/v1", "bad-key")

    assert result.ok is False
    assert result.status == 401
    assert result.error == '{"error":{"message":"Invalid API key"}}'
