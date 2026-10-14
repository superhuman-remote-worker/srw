"""NoCookieJar keeps Nextcloud service clients from echoing any cookie."""

from __future__ import annotations

import email.message
import io
from types import SimpleNamespace

import httpx
import pytest
import requests
from requests.adapters import BaseAdapter
from requests.structures import CaseInsensitiveDict

from shared.http_no_cookies import NoCookieJar

SET_COOKIES = [
    ("set-cookie", "cookie_test=test; path=/"),
    ("set-cookie", "oc_sessionPassphrase=secret; path=/; secure; HttpOnly"),
    ("set-cookie", "nc_session_id=abc; path=/; secure; HttpOnly"),
]


def _sent_cookie_headers_httpx(client_cls):
    seen: list[str | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.headers.get("cookie"))
        return httpx.Response(200, headers=SET_COOKIES)

    return seen, client_cls(
        transport=httpx.MockTransport(handler), cookies=NoCookieJar()
    )


def test_httpx_client_never_sends_a_cookie():
    seen, client = _sent_cookie_headers_httpx(httpx.Client)
    with client:
        client.get("http://srw-nextcloud/status.php")
        client.get("http://srw-nextcloud/status.php")
        assert len(client.cookies.jar) == 0
    assert seen == [None, None]


@pytest.mark.asyncio
async def test_httpx_async_client_never_sends_a_cookie():
    seen, client = _sent_cookie_headers_httpx(httpx.AsyncClient)
    async with client:
        await client.get("http://srw-nextcloud/status.php")
        await client.get("http://srw-nextcloud/status.php")
        assert len(client.cookies.jar) == 0
    assert seen == [None, None]


class _RawResponse(io.BytesIO):
    """Just enough of urllib3's response for requests to read its cookies."""

    def __init__(self) -> None:
        super().__init__(b"")
        self._original_response = SimpleNamespace(msg=email.message.Message())
        for name, value in SET_COOKIES:
            self._original_response.msg[name] = value


class _CookieSettingAdapter(BaseAdapter):
    def __init__(self, seen: list[str | None]) -> None:
        super().__init__()
        self.seen = seen

    def send(self, request, **kwargs):  # noqa: ANN001, ANN003
        self.seen.append(request.headers.get("Cookie"))
        response = requests.Response()
        response.status_code = 200
        response.headers = CaseInsensitiveDict(dict(SET_COOKIES[:1]))
        response.raw = _RawResponse()
        response.url = request.url
        response.request = request
        return response

    def close(self) -> None:
        pass


def test_requests_session_never_sends_a_cookie():
    seen: list[str | None] = []
    session = requests.Session()
    session.cookies = NoCookieJar()
    session.mount("http://", _CookieSettingAdapter(seen))
    session.get("http://srw-nextcloud/status.php")
    session.get("http://srw-nextcloud/status.php")
    assert seen == [None, None]
    assert len(session.cookies) == 0


def test_requests_default_jar_would_echo_cookie_test():
    """Control: the default jar sends ``cookie_test`` back, which is the bug."""
    seen: list[str | None] = []
    session = requests.Session()
    session.mount("http://", _CookieSettingAdapter(seen))
    session.get("http://srw-nextcloud/status.php")
    session.get("http://srw-nextcloud/status.php")
    assert seen[0] is None
    assert seen[1] == "cookie_test=test"
