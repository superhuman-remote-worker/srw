"""Calls from Asterisk over ARI; knowledge-base/knowledge/features/local_voice_gateway.md §4.2.

One AsteriskCallSource serves every line of the addon. The rendered dialplan
puts each incoming channel into the Stasis app with the line's name; the
source keeps it ringing and announces it through ``Gateway.incoming_call``.

- accept: answer, bridge the caller with an externalMedia channel over
  chan_websocket, and hand the engine that channel's audio.
- reject: hang up before answer with the Q.850 cause chan_pjsip turns into the
  requested SIP status.
- refer: the caller leaves Stasis for the transfer context, whose Transfer()
  sends a blind REFER and returns with the result. If the PBX refuses, the
  source dials the target through the same line while the caller stays with
  the voice; once the target answers, the caller leaves the app for the
  hand-over context, whose Bridge() joins it with the target outside
  Stasis, and the source forgets the call: a gateway restart or rollout
  never drops a handed-over call. (ARI's own redirect sends the REFER too,
  but reports no outcome and reads frames from a bridged channel's control
  thread.)
- DTMF arrives as ChannelDtmfReceived whichever way the PBX sent it.

Logs carry session ids, lines and outcomes; never numbers or SIP headers.
"""

from __future__ import annotations

import asyncio
import ipaddress
import logging
import re
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

import aiohttp

from voice_gateway.asterisk import (
    DEFAULT_APP,
    DEFAULT_MEDIA_CONNECTION,
    DIAL_ARG,
    ENDPOINT_PREFIX,
    HANDOVER_CONTEXT,
    HANDOVER_VAR,
    PBX_VAR,
    REFER_TO_VAR,
    TRANSFER_CONTEXT,
)
from voice_gateway.asterisk.ari import AriClient, AriError
from voice_gateway.asterisk.media import MEDIA_FORMAT, MediaPort, MediaServer
from voice_gateway.errors import ConfigError
from voice_gateway.session import ProtocolError

if TYPE_CHECKING:
    from voice_gateway.server import Gateway
    from voice_gateway.session import Session

logger = logging.getLogger(__name__)

#: A reject's SIP status -> the Q.850 cause chan_pjsip maps back to that status
#: (chan_pjsip.c hangup_cause2sip; a cause it does not map becomes 603).
REJECT_CAUSES = {
    403: 21,
    404: 1,
    408: 18,
    410: 22,
    480: 19,
    484: 28,
    486: 17,
    488: 58,
    500: 38,
    501: 29,
    502: 27,
    503: 34,
    603: 16,
}
#: Headers the webhook carries when the PBX sent them, in this order.
SIP_HEADERS = (
    ("From", 1),
    ("To", 1),
    ("P-Asserted-Identity", 1),
    ("Diversion", 8),
    ("History-Info", 8),
)
#: Asterisk's ``;line=`` token in a registered line's Contact. The PBX echoes
#: it in To and History-Info; it identifies an INVITE as that line's, so it
#: stays in the addon instead of reaching the orchestrator's call records.
_LINE_PARAM = re.compile(r";line=[^;>?,\s]*", re.IGNORECASE)
_SIP_URI = re.compile(r"sips?:[A-Za-z0-9\-_.~*%+;=:@\[\]]{1,200}")
_TEL_URI = re.compile(r"tel:(\+?[0-9*#][0-9*#\-. ]{0,40})")
_FAILURES = (AriError, aiohttp.ClientError, TimeoutError)


def reject_cause(status: int) -> int:
    """The hangup cause for a reject; other codes fall back to their class."""
    if status in REJECT_CAUSES:
        return REJECT_CAUSES[status]
    if status >= 600:
        return REJECT_CAUSES[603]
    if status >= 500:
        return REJECT_CAUSES[500]
    return REJECT_CAUSES[480]


def refer_uri(target: str, pbx: str | None) -> str:
    """The SIP URI to transfer to; a tel: number goes to the line's PBX.

    The URI ends up in a dialplan argument, so anything that could escape it
    (commas, ``$``, quotes, braces, whitespace) is refused.
    """
    target = target.strip()
    if _SIP_URI.fullmatch(target):
        return target
    match = _TEL_URI.fullmatch(target)
    if match and pbx:
        return "sip:" + re.sub(r"[\-. ]", "", match.group(1)) + "@" + pbx
    raise ProtocolError(
        400, "invalid_target_uri", "target_uri must be a sip: URI or a tel: number"
    )


@dataclass(frozen=True)
class AsteriskSettings:
    ari_url: str
    ari_user: str
    # Shared by ARI (gateway -> Asterisk) and the media listener
    # (Asterisk -> gateway); the renderer writes it into both configs.
    password: str = field(repr=False)
    app: str = DEFAULT_APP
    media_listen: str = "127.0.0.1:18441"
    media_connection: str = DEFAULT_MEDIA_CONNECTION
    media_timeout_s: float = 5.0
    dial_timeout_s: int = 30


class Call:
    """One incoming channel and what the source added to it."""

    def __init__(self, line: str, caller: str, caller_number: str | None):
        self.line, self.caller, self.caller_number = line, caller, caller_number
        self.session: Session | None = None
        self.pbx: str | None = None
        self.media = self.bridge = self.dial = None
        self.port: MediaPort | None = None
        # ringing -> answered <-> transferring; answered -> handed_over (the
        # dialplan bridges caller and target; the source lets go) | ended
        self.state = "ringing"
        # The gateway is ending the call, so losing its channels is expected.
        self.ending = False
        self.refer_to: str | None = None
        self.events: asyncio.Queue[dict] = asyncio.Queue()
        self.waiters: dict[str, asyncio.Future] = {}

    @property
    def sid(self) -> str:
        return self.session.id if self.session else "-"

    def started(self, channel: str) -> asyncio.Future:
        future = asyncio.get_running_loop().create_future()
        self.waiters[channel] = future
        return future

    def notice(self, event: dict) -> None:
        """Wake an answer() waiting on a channel, without the event queue
        (whose consumer may be waiting for the session lock answer holds)."""
        channel = event["channel"]["id"]
        kind = event.get("type")
        if kind == "StasisStart" and channel in self.waiters:
            future = self.waiters.pop(channel)
            if not future.done():
                future.set_result(None)
        elif kind == "ChannelDestroyed":
            gone = self.waiters if channel == self.caller else {}
            if channel in self.waiters:
                gone = {channel: self.waiters[channel], **gone}
            for key, future in list(gone.items()):
                self.waiters.pop(key, None)
                if not future.done():
                    future.set_exception(ConnectionError("channel destroyed"))


async def read_headers(ari: AriClient, channel: str) -> list[dict]:
    headers = []
    for name, most in SIP_HEADERS:
        for index in range(1, most + 1):
            value = await ari.variable(channel, f"PJSIP_HEADER(read,{name},{index})")
            if value is None:
                break
            headers.append({"name": name, "value": _LINE_PARAM.sub("", value)})
    return headers


def _created(value) -> float | None:
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%f%z").timestamp()
    except (TypeError, ValueError):
        return None


class AsteriskCallSource:
    """CallSource over ARI; runs as long as the gateway's app (start/stop)."""

    def __init__(self, settings: AsteriskSettings):
        self.settings = settings
        self.ari = AriClient(
            settings.ari_url, settings.ari_user, settings.password, settings.app
        )
        self.media = MediaServer(
            settings.media_listen,
            settings.ari_user,
            settings.password,
            start_timeout_s=settings.media_timeout_s,
        )
        self.gateway: Gateway | None = None
        self.calls: dict[str, Call] = {}  # by session id
        self.channels: dict[str, Call] = {}  # by every channel id of a call
        self.tasks: set[asyncio.Task] = set()
        self.events: asyncio.Task | None = None

    # -- lifecycle -----------------------------------------------------------------

    async def start(self, gateway: Gateway) -> None:
        self.gateway = gateway
        await self.ari.open()
        await self.media.start()
        self.events = asyncio.create_task(
            self.ari.run_events(self.on_event, self.reconcile)
        )

    async def stop(self) -> None:
        if self.events is not None:
            self.events.cancel()
            await asyncio.gather(self.events, return_exceptions=True)
        for call in set(self.channels.values()):
            call.ending = True
            try:
                await asyncio.wait_for(self.release(call, caller=True), 5)
            except _FAILURES:
                pass
        tasks = tuple(self.tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await self.media.stop()
        await self.ari.close()

    def spawn(self, coroutine) -> asyncio.Task:
        task = asyncio.create_task(coroutine)
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)
        return task

    # -- events ----------------------------------------------------------------------

    def on_event(self, event: dict) -> None:
        channel = (event.get("channel") or {}).get("id")
        if not isinstance(channel, str):
            return
        call = self.channels.get(channel)
        if call is not None:
            call.notice(event)
            call.events.put_nowait(event)
        elif event.get("type") == "StasisStart":
            args = event.get("args") or []
            if len(args) == 1 and isinstance(args[0], str) and args[0] != DIAL_ARG:
                caller = (event["channel"].get("caller") or {}).get("number")
                call = Call(args[0], channel, caller or None)
                self.channels[channel] = call
                self.spawn(self.run(call))
            else:
                # Audio or a dial leg whose call has ended, or a caller back
                # from the transfer context after its call was forgotten.
                self.spawn(self.hang_up(channel))

    async def reconcile(self) -> None:
        """After every (re)connect: close calls whose channels vanished while
        the events socket was down, and hang up calls an earlier gateway run
        left in the app."""
        connected = time.time()
        # Only channels known before the listing: a call that arrives while
        # it is in flight is not in it, yet alive.
        known = dict(self.channels)
        try:
            channels = await self.ari.channels()
        except _FAILURES:
            return
        alive = {c.get("id") for c in channels if isinstance(c, dict)}
        for channel, call in known.items():
            # A media or dial channel may not exist yet; only a vanished
            # caller, or audio whose socket is gone too, ends a call.
            lost_audio = (
                channel == call.media and call.port and call.port.ended.is_set()
            )
            if (
                channel not in alive
                and self.channels.get(channel) is call
                and (channel == call.caller or lost_audio)
            ):
                lost = {"type": "ChannelDestroyed", "channel": {"id": channel}}
                call.notice(lost)
                call.events.put_nowait(lost)
        for item in channels:
            dialplan = item.get("dialplan") or {}
            created = _created(item.get("creationtime"))
            if (
                item.get("id") not in self.channels
                and dialplan.get("app_name") == "Stasis"
                and str(dialplan.get("app_data", "")).split(",")[0] == self.settings.app
                and created is not None
                and created < connected - 2
            ):
                logger.warning("hanging up a call an earlier gateway run left behind")
                await self.hang_up(item["id"])

    async def run(self, call: Call) -> None:
        try:
            if await self.ring(call):
                while not await self.handle(call, await call.events.get()):
                    pass
        except Exception as exc:  # noqa: BLE001 - one call must not stop the source
            logger.error("call %s failed: %s", call.sid, type(exc).__name__)
            await self.end(call, "connection_lost", caller=True)
        finally:
            for channel in [c for c, owner in self.channels.items() if owner is call]:
                self.channels.pop(channel, None)
            if call.session is not None:
                self.calls.pop(call.session.id, None)
            if call.media:
                self.media.forget(call.media)

    async def ring(self, call: Call) -> bool:
        """Ring the caller and announce the call; False if it ended first."""
        try:
            # Stay subscribed while the caller runs the transfer dialplan.
            await self.ari.subscribe_channel(call.caller)
            await self.ari.ring(call.caller)
            headers = await read_headers(self.ari, call.caller)
            call.pbx = await self.ari.variable(call.caller, PBX_VAR)
        except _FAILURES as exc:
            logger.info("call on line %s ended before it rang: %s", call.line, exc)
            await self.hang_up(call.caller)
            return False
        try:
            session = self.gateway.incoming_call(call.line, headers)
        except ProtocolError:
            logger.error("Asterisk sent a call for unknown line %s", call.line)
            await self.hang_up(call.caller, reject_cause(480))
            return False
        call.session = session
        call.media = session.id + "-media"
        call.bridge = session.id + "-bridge"
        self.calls[session.id] = call
        return True

    async def handle(self, call: Call, event: dict) -> bool:
        """Apply one event; True once the call is over for the source."""
        kind, channel = event.get("type"), event["channel"]["id"]
        session = call.session
        if kind == "ChannelDtmfReceived":
            digit = event.get("digit")
            if (
                channel == call.caller
                and call.state == "answered"
                and session.state == "active"
                and isinstance(digit, str)
                and len(digit) == 1
            ):
                session.dtmf(digit)
        elif kind == "StasisStart":
            args = event.get("args") or []
            if channel == call.caller and args[1:2] == ["transfer"]:
                await self.transfer_result(call, args[2:])
            elif channel == call.dial:
                return await self.hand_over(call)
        elif kind == "ChannelDestroyed":
            if channel == call.caller:
                if not call.ending:
                    logger.info("call %s: caller hung up", call.sid)
                await self.end(call, "remote_hangup", caller=False)
                return True
            if channel == call.dial:
                logger.info("call %s: transfer target not reached", call.sid)
                call.dial = None
            elif channel == call.media and call.state in ("answered", "transferring"):
                if not call.ending:
                    logger.warning("call %s: Asterisk dropped the audio", call.sid)
                    await self.end(call, "connection_lost", caller=True)
                    return True
        return False

    # -- CallSource --------------------------------------------------------------------

    def call(self, session: Session) -> Call:
        call = self.calls.get(session.id)
        if call is None or call.state == "ended":
            raise ProtocolError(409, "session_not_active", "the call has ended")
        return call

    async def answer(self, session: Session) -> None:
        call = self.call(session)
        port = MediaPort(call.media)
        self.media.expect(port)
        self.channels[call.media] = call
        entered = call.started(call.media)
        try:
            await self.ari.answer(call.caller)
            await self.ari.create_bridge(call.bridge)
            await self.ari.add_to_bridge(call.bridge, call.caller)
            await self.ari.external_media(
                call.media, self.settings.media_connection, MEDIA_FORMAT
            )
            # chan_websocket connects to websocket_client.conf's URI, sends
            # MEDIA_START, auto-answers, and only then enters the app. ARI
            # answers 200 even when that connection later fails.
            await asyncio.wait_for(
                asyncio.gather(entered, port.started.wait()),
                self.settings.media_timeout_s,
            )
            await self.ari.add_to_bridge(call.bridge, call.media)
        except (*_FAILURES, ConnectionError) as exc:
            entered.cancel()
            if isinstance(exc, (TimeoutError, ConnectionError)):
                # The media channel gave up (Asterisk logs "WebSocket
                # connection failed") or never came.
                logger.error(
                    "call %s: no audio (%s); does websocket_client.conf entry %s "
                    "point at this gateway's media listener (%s)?",
                    call.sid,
                    "timeout" if isinstance(exc, TimeoutError) else "channel ended",
                    self.settings.media_connection,
                    self.settings.media_listen,
                )
            else:
                logger.error(
                    "call %s: answer failed (%s)", call.sid, type(exc).__name__
                )
            await self.end(call, None, caller=True)
            await session.close("connection_lost")
            raise ProtocolError(
                502, "media_unavailable", "Asterisk could not start the call's audio"
            ) from None
        call.port = port
        call.state = "answered"
        session.audio = port
        logger.info("call %s answered on line %s", call.sid, call.line)

    async def reject(self, session: Session, status_code: int) -> None:
        call = self.calls.get(session.id)
        if call is not None:
            call.ending = True
            await self.hang_up(call.caller, reject_cause(status_code))

    async def refer(self, session: Session, target_uri: str) -> None:
        call = self.call(session)
        if call.state != "answered" or call.dial is not None:
            raise ProtocolError(
                409, "transfer_in_progress", "a transfer is already under way"
            )
        call.refer_to = refer_uri(target_uri, call.pbx)
        call.state = "transferring"
        try:
            await self.ari.set_variable(call.caller, REFER_TO_VAR, call.refer_to)
            await self.ari.continue_in_dialplan(call.caller, TRANSFER_CONTEXT)
        except _FAILURES:
            call.state = "answered"
            raise ProtocolError(
                502, "transfer_failed", "Asterisk did not start the transfer"
            ) from None
        logger.info("call %s: blind transfer sent", call.sid)

    async def hangup(self, session: Session) -> None:
        call = self.calls.get(session.id)
        if call is not None:
            call.ending = True
            await self.release(call, caller=True)

    # -- transfer ------------------------------------------------------------------------

    async def transfer_result(self, call: Call, result: list) -> None:
        status = result[0] if result else "FAILURE"
        code = result[1] if len(result) > 1 else "0"
        if call.state != "transferring" or call.ending:
            await self.hang_up(call.caller)
            return
        if status == "SUCCESS":
            # The PBX has the caller now; it usually sends BYE itself.
            logger.info("call %s: transfer accepted by the PBX", call.sid)
            await self.end(call, "remote_hangup", caller=True)
            return
        logger.info(
            "call %s: PBX refused the transfer (%s); dialling the target on line %s",
            call.sid,
            code,
            call.line,
        )
        call.state = "answered"
        call.dial = call.session.id + "-dial"
        self.channels[call.dial] = call
        try:
            await self.ari.add_to_bridge(call.bridge, call.caller)
            await self.ari.originate(
                call.dial,
                f"PJSIP/{ENDPOINT_PREFIX}{call.line}/{call.refer_to}",
                app_args=DIAL_ARG,
                caller_id=call.caller_number,
                timeout_s=self.settings.dial_timeout_s,
            )
        except _FAILURES:
            logger.warning("call %s: could not dial the transfer target", call.sid)
            self.channels.pop(call.dial, None)
            call.dial = None

    async def hand_over(self, call: Call) -> bool:
        """The target answered the fallback dial: end the voice and hand the
        caller to the target outside the app; True once the source let go.

        The caller leaves Stasis for the hand-over context, whose Bridge()
        pulls the answered dial leg out of the app and joins the two; the
        bridge is Asterisk's own, so stopping or restarting the gateway
        leaves the call alone. The source forgets the call's channels."""
        session = call.session
        async with session.lock:
            if call.state != "answered" or call.ending or session.state != "active":
                await self.hang_up(call.dial)
                return False
            call.state = "handed_over"
            try:
                await self.ari.set_variable(call.caller, HANDOVER_VAR, call.dial)
                await self.hang_up(call.media)
                await self.ari.continue_in_dialplan(call.caller, HANDOVER_CONTEXT)
            except _FAILURES:
                # The caller may still sit in the voice's bridge (or has hung
                # up meanwhile): end the call rather than leave either leg.
                logger.warning("call %s: hand-over failed", call.sid)
                call.ending, call.state = True, "ended"
                await self.release(call, caller=True)
                await session.close("connection_lost")
                return True
            if call.port is not None:
                await call.port.close()
            try:
                await self.ari.destroy_bridge(call.bridge)
            except _FAILURES:
                pass
            for channel in (call.caller, call.media, call.dial):
                self.channels.pop(channel, None)
            call.ending = True
            logger.info("call %s: caller handed to the transfer target", call.sid)
            await session.close("remote_hangup")
            return True

    # -- teardown ------------------------------------------------------------------------

    async def hang_up(self, channel: str | None, cause: int | None = None) -> None:
        if channel:
            try:
                await self.ari.hangup(channel, cause)
            except _FAILURES:
                pass

    async def release(self, call: Call, *, caller: bool) -> None:
        """Hang up what the source added to a call, and the caller if asked."""
        await self.hang_up(call.media)
        await self.hang_up(call.dial)
        if caller:
            await self.hang_up(call.caller)
        if call.bridge:
            try:
                await self.ari.destroy_bridge(call.bridge)
            except _FAILURES:
                pass
        if call.port is not None:
            await call.port.close()

    async def end(self, call: Call, reason: str | None, *, caller: bool) -> None:
        if call.state == "ended":
            return
        call.ending = True
        call.state = "ended"
        await self.release(call, caller=caller)
        session = call.session
        if reason and session is not None and session.state != "closed":
            async with session.lock:
                await session.close(reason)


# -- settings ---------------------------------------------------------------------------------


def _text(settings: dict, name: str, default: str | None = None) -> str:
    value = settings.get(name, default)
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"source.{name} must be a nonempty string")
    return value.strip()


def _listen(value: str) -> str:
    host, _, port = value.rpartition(":")
    try:
        if host != "localhost":
            ipaddress.ip_address(host.strip("[]"))
        if not 0 < int(port) < 65536:
            raise ValueError
    except ValueError:
        raise ConfigError(
            "source.media_listen must be an IP address (or localhost) and port"
        ) from None
    return value


def asterisk_kind(settings: dict, base: Path) -> Callable[[], AsteriskCallSource]:
    """The ``asterisk`` source kind: checks its settings, reads its secret."""
    allowed = {
        "ari_url",
        "ari_user",
        "password_file",
        "app",
        "media_listen",
        "media_connection",
        "media_timeout_s",
        "dial_timeout_s",
    }
    if settings.keys() - allowed:
        raise ConfigError("source contains unknown asterisk settings")
    ari_url = _text(settings, "ari_url").rstrip("/")
    try:
        url = urlsplit(ari_url)
        if (
            url.scheme not in {"http", "https"}
            or not url.hostname
            or url.username
            or url.path
            or url.query
            or url.fragment
        ):
            raise ValueError
        _ = url.port
    except ValueError:
        raise ConfigError(
            "source.ari_url must be Asterisk's http(s) base URL without a path or credentials"
        ) from None
    try:
        password = (base / _text(settings, "password_file")).read_text().strip()
    except (OSError, UnicodeError):
        raise ConfigError("cannot read source.password_file") from None
    if not password or any(c in password for c in ' \t\n"\\'):
        raise ConfigError(
            "source.password_file must hold a password without spaces or quotes"
        )
    timeouts = {}
    for name, default, low, high in (
        ("media_timeout_s", 5.0, 0.5, 30.0),
        ("dial_timeout_s", 30, 5, 120),
    ):
        value = settings.get(name, default)
        if type(value) not in (int, float) or not low <= value <= high:
            raise ConfigError(f"source.{name} must be a number from {low} to {high}")
        timeouts[name] = value
    parsed = AsteriskSettings(
        ari_url=ari_url,
        ari_user=_text(settings, "ari_user"),
        password=password,
        app=_text(settings, "app", DEFAULT_APP),
        media_listen=_listen(_text(settings, "media_listen", "127.0.0.1:18441")),
        media_connection=_text(settings, "media_connection", DEFAULT_MEDIA_CONNECTION),
        media_timeout_s=float(timeouts["media_timeout_s"]),
        dial_timeout_s=int(timeouts["dial_timeout_s"]),
    )
    return lambda: AsteriskCallSource(parsed)
