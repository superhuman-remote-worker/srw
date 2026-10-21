"""Echo engine; knowledge-base/knowledge/features/local_voice_gateway.md §6.1 (WP2 gate).

Plays the caller's audio straight back, so a test caller hears its own words
and proves both audio directions through the PBX, Asterisk and the gateway.
It speaks no appends (no TTS) and keeps nothing: each frame goes back out as
soon as it arrives.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from voice_gateway.errors import ConfigError

if TYPE_CHECKING:
    from voice_gateway.interfaces import AudioPort
    from voice_gateway.session import Session


class EchoEngine:
    def __init__(self) -> None:
        self.task: asyncio.Task | None = None

    async def start(self, session: Session, accept_body: dict) -> None:
        # A call without audio (the scripted source) has nothing to echo.
        if session.audio is not None:
            self.task = asyncio.create_task(self.echo(session.audio))

    async def echo(self, audio: AudioPort) -> None:
        async for frame in audio.frames():
            await audio.play(frame)

    async def append(
        self, session: Session, kind: str, content: str, delegation_id: str | None
    ) -> None:
        pass

    async def stop(self, session: Session) -> None:
        if self.task is not None:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
            self.task = None


def echo_kind(name: str, settings: dict) -> type[EchoEngine]:
    """The ``echo`` profile kind; it takes no settings."""
    if settings:
        raise ConfigError(f"profile {name}: the echo engine takes no settings")
    return EchoEngine
