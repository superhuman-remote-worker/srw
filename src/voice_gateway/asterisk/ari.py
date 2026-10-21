"""Asterisk REST Interface client; knowledge-base/knowledge/features/local_voice_gateway.md §4.2.

REST calls and the events WebSocket of one Stasis application. Credentials go
in an HTTP basic auth header, never as ``api_key`` in a URL, which Asterisk
and proxies may log. The events socket reconnects with backoff; after every
connect the owner's ``on_connect`` hook reconciles what it may have missed.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from typing import Any

import aiohttp

logger = logging.getLogger(__name__)

EventHandler = Callable[[dict], None]


class AriError(Exception):
    """A failed ARI request; carries the status, never a request body."""

    def __init__(self, status: int, method: str, path: str):
        super().__init__(f"ARI {method} {path} answered {status}")
        self.status = status


def _query(params: dict[str, Any]) -> dict[str, str]:
    out = {}
    for key, value in params.items():
        if value is None:
            continue
        if isinstance(value, bool):
            value = "true" if value else "false"
        elif isinstance(value, (list, tuple)):
            value = ",".join(str(v) for v in value)
        out[key] = str(value)
    return out


class AriClient:
    """One Stasis application's view of Asterisk."""

    def __init__(
        self,
        base_url: str,
        user: str,
        password: str,
        app: str,
        *,
        timeout_s: float = 5.0,
    ):
        self.base = base_url.rstrip("/") + "/ari"
        self.app = app
        self.auth = aiohttp.BasicAuth(user, password)
        self.timeout = aiohttp.ClientTimeout(total=timeout_s)
        self.http: aiohttp.ClientSession | None = None
        self.connected = asyncio.Event()

    async def open(self) -> None:
        if self.http is None:
            self.http = aiohttp.ClientSession(auth=self.auth, timeout=self.timeout)

    async def close(self) -> None:
        if self.http is not None:
            await self.http.close()
            self.http = None

    async def request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        body: dict | None = None,
        missing_ok: bool = False,
    ) -> Any:
        """Call ARI; ``missing_ok`` turns 404 (channel/bridge gone) into None."""
        assert self.http is not None, "AriClient.open() first"
        async with self.http.request(
            method,
            self.base + path,
            params=_query(params or {}),
            json=body,
            allow_redirects=False,
        ) as response:
            if response.status == 404 and missing_ok:
                return None
            if response.status >= 300:
                raise AriError(response.status, method, path)
            text = await response.text()
            return json.loads(text) if text else None

    # -- the calls the source uses --------------------------------------------------

    async def ring(self, channel: str) -> None:
        await self.request("POST", f"/channels/{channel}/ring")

    async def answer(self, channel: str) -> None:
        await self.request("POST", f"/channels/{channel}/answer")

    async def hangup(self, channel: str, cause: int | None = None) -> None:
        """Hang up; ``cause`` is a Q.850 code chan_pjsip maps to a SIP status."""
        await self.request(
            "DELETE",
            f"/channels/{channel}",
            params={"reason_code": cause},
            missing_ok=True,
        )

    async def variable(self, channel: str, name: str) -> str | None:
        """A channel variable or dialplan function; None when unset or gone."""
        try:
            result = await self.request(
                "GET",
                f"/channels/{channel}/variable",
                params={"variable": name},
                missing_ok=True,
            )
        except AriError as exc:
            # A function with nothing to read fails with 500 "Error With
            # Function" (PJSIP_HEADER for a header the INVITE lacks).
            if exc.status == 500 and name.endswith(")"):
                return None
            raise
        value = (result or {}).get("value")
        return value if isinstance(value, str) and value else None

    async def set_variable(self, channel: str, name: str, value: str) -> None:
        await self.request(
            "POST",
            f"/channels/{channel}/variable",
            params={"variable": name, "value": value},
        )

    async def create_bridge(self, bridge: str) -> None:
        await self.request(
            "POST", f"/bridges/{bridge}", params={"type": "mixing", "name": bridge}
        )

    async def add_to_bridge(self, bridge: str, *channels: str) -> None:
        await self.request(
            "POST", f"/bridges/{bridge}/addChannel", params={"channel": channels}
        )

    async def remove_from_bridge(self, bridge: str, channel: str) -> None:
        try:
            await self.request(
                "POST",
                f"/bridges/{bridge}/removeChannel",
                params={"channel": channel},
                missing_ok=True,
            )
        except AriError as exc:
            if exc.status != 422:  # not in that bridge any more
                raise

    async def destroy_bridge(self, bridge: str) -> None:
        await self.request("DELETE", f"/bridges/{bridge}", missing_ok=True)

    async def external_media(
        self, channel: str, connection: str, audio_format: str
    ) -> None:
        """A chan_websocket channel that connects out to ``connection``, an
        entry of websocket_client.conf (not a URL)."""
        await self.request(
            "POST",
            "/channels/externalMedia",
            params={
                "app": self.app,
                "channelId": channel,
                "external_host": connection,
                "transport": "websocket",
                "encapsulation": "none",
                "connection_type": "client",
                "format": audio_format,
            },
        )

    async def originate(
        self,
        channel: str,
        endpoint: str,
        *,
        app_args: str,
        caller_id: str | None,
        timeout_s: int,
    ) -> None:
        """Dial ``endpoint``; the channel enters this app once answered."""
        await self.request(
            "POST",
            f"/channels/{channel}",
            params={
                "endpoint": endpoint,
                "app": self.app,
                "appArgs": app_args,
                "callerId": caller_id,
                "timeout": timeout_s,
            },
        )

    async def continue_in_dialplan(self, channel: str, context: str) -> None:
        await self.request(
            "POST",
            f"/channels/{channel}/continue",
            params={"context": context, "extension": "s", "priority": 1},
        )

    async def subscribe_channel(self, channel: str) -> None:
        """Keep receiving a channel's events while it runs dialplan outside
        Stasis (the transfer context), not only while it is in the app."""
        await self.request(
            "POST",
            f"/applications/{self.app}/subscription",
            params={"eventSource": f"channel:{channel}"},
            missing_ok=True,
        )

    async def channels(self) -> list[dict]:
        return await self.request("GET", "/channels") or []

    # -- events ---------------------------------------------------------------------

    async def run_events(
        self,
        handler: EventHandler,
        on_connect: Callable[[], Awaitable[None]],
        *,
        max_delay_s: float = 5.0,
    ) -> None:
        """Receive the app's events until cancelled, reconnecting on loss."""
        assert self.http is not None, "AriClient.open() first"
        url = self.base.replace("http", "ws", 1) + "/events"
        delay = 0.5
        while True:
            try:
                async with self.http.ws_connect(
                    url,
                    params={"app": self.app, "subscribeAll": "false"},
                    heartbeat=20,
                    timeout=aiohttp.ClientWSTimeout(ws_close=5),
                    max_msg_size=4 * 1024 * 1024,
                ) as ws:
                    delay = 0.5
                    self.connected.set()
                    logger.info("ARI app %s connected", self.app)
                    reconcile = asyncio.create_task(on_connect())
                    try:
                        async for message in ws:
                            if message.type != aiohttp.WSMsgType.TEXT:
                                continue
                            try:
                                event = json.loads(message.data)
                            except ValueError:
                                continue
                            if isinstance(event, dict):
                                handler(event)
                    finally:
                        reconcile.cancel()
                        await asyncio.gather(reconcile, return_exceptions=True)
            except (aiohttp.ClientError, OSError, TimeoutError) as exc:
                logger.warning(
                    "ARI events unavailable (%s); retrying", type(exc).__name__
                )
            finally:
                self.connected.clear()
            await asyncio.sleep(delay)
            delay = min(delay * 2, max_delay_s)
