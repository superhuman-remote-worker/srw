"""Scripted call source and engine; knowledge-base/knowledge/features/local_voice_gateway.md §4.1."""

from __future__ import annotations

from typing import TYPE_CHECKING

from voice_gateway.errors import ConfigError

if TYPE_CHECKING:
    from voice_gateway.session import Session


class ScriptedCallSource:
    """No physical call; the test API supplies the incoming and hangup edges."""

    async def answer(self, session: Session) -> None:
        pass

    async def reject(self, session: Session, status_code: int) -> None:
        pass

    async def refer(self, session: Session, target_uri: str) -> None:
        pass

    async def hangup(self, session: Session) -> None:
        pass


class ScriptedEngine:
    """No audio or autonomous speech. Speaking appends become output transcript."""

    async def start(self, session: Session, accept_body: dict) -> None:
        pass

    async def append(
        self, session: Session, kind: str, content: str, delegation_id: str | None
    ) -> None:
        if kind != "thinking":
            session.transcript("assistant", content)

    async def stop(self, session: Session) -> None:
        pass


def scripted_kind(name: str, settings: dict) -> type[ScriptedEngine]:
    """The ``scripted`` profile kind; it takes no settings."""
    if settings:
        raise ConfigError(f"profile {name}: the scripted engine takes no settings")
    return ScriptedEngine
