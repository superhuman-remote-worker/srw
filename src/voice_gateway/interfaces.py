"""Call-source and engine ports; knowledge-base/knowledge/features/local_voice_gateway.md §4.2–4.3."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from voice_gateway.session import Session


#: Audio between a call source and an engine: 16 kHz mono PCM16 little-endian
#: in 20 ms frames, the format Asterisk's chan_websocket delivers as slin16.
SAMPLE_RATE = 16_000
FRAME_BYTES = 640


class AudioPort(Protocol):
    """The call's audio as an engine sees it. The source sets ``Session.audio``
    when it answers; a call without audio (the scripted source) leaves it None."""

    def frames(self) -> AsyncIterator[bytes]:
        """Caller audio in FRAME_BYTES frames until the call's media ends."""
        ...

    async def play(self, pcm: bytes) -> None:
        """Queue audio for the caller; the port frames and paces it."""
        ...

    async def flush(self) -> None:
        """Drop queued audio the caller has not heard yet (barge-in)."""
        ...

    def played_ms(self) -> int:
        """Milliseconds of queued audio actually played to the caller so far."""
        ...


class CallSource(Protocol):
    """The real call's controls. Sources enter through Gateway.incoming_call()."""

    async def answer(self, session: Session) -> None: ...
    async def reject(self, session: Session, status_code: int) -> None: ...
    async def refer(self, session: Session, target_uri: str) -> None: ...
    async def hangup(self, session: Session) -> None: ...


class Engine(Protocol):
    """One engine per call; emit through Session's transcript/delegate/dtmf/usage."""

    async def start(self, session: Session, accept_body: dict) -> None: ...
    async def append(
        self, session: Session, kind: str, content: str, delegation_id: str | None
    ) -> None: ...
    async def stop(self, session: Session) -> None: ...
