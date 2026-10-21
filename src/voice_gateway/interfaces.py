"""Call-source and engine ports; knowledge-base/knowledge/features/local_voice_gateway.md §4.2–4.3."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from voice_gateway.session import Session


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
