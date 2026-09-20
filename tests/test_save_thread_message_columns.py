import pytest
from unittest.mock import AsyncMock, MagicMock
from orchestrator.database.postgres import PostgresDB


@pytest.mark.asyncio
async def test_save_thread_message_inserts_component_columns():
    db = PostgresDB.__new__(PostgresDB)
    conn = AsyncMock()
    conn.fetchrow = AsyncMock(return_value={"id": "abc"})
    conn.execute = AsyncMock()
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=conn)
    cm.__aexit__ = AsyncMock(return_value=False)
    db.acquire = MagicMock(return_value=cm)
    transaction = MagicMock()
    transaction.__aenter__ = AsyncMock(return_value=None)
    transaction.__aexit__ = AsyncMock(return_value=False)
    conn.transaction = MagicMock(return_value=transaction)

    await db.save_thread_message(
        thread_id="t1",
        role="ai",
        content="hi",
        turn_number=1,
        provider="openai-chat",
        provider_raw={"object": "chat.completion", "id": "cc_1"},
        reasoning="thinking",
    )

    sql = conn.fetchrow.call_args[0][0]
    assert "provider" in sql and "provider_raw" in sql and "reasoning" in sql
    # JSON-encoded provider_raw reached the positional args
    assert any(isinstance(a, str) and "cc_1" in a for a in conn.fetchrow.call_args[0])


def test_request_model_accepts_component_fields():
    # Import inside the test body so any module-level import errors in
    # orchestrator.main do not prevent the asyncio DB test above from being
    # collected and run.  We also set minimal env-vars that main.py requires
    # at import time (VECTOR_DB_URL, aiosmtplib stub) so the import succeeds
    # without a real database or SMTP connection.
    import os
    import sys
    from unittest.mock import MagicMock

    os.environ.setdefault("VECTOR_DB_URL", "postgresql://x:x@localhost/x")
    sys.modules.setdefault("aiosmtplib", MagicMock())

    from orchestrator.main import AgentThreadMessageRequest  # noqa: PLC0415

    m = AgentThreadMessageRequest(
        role="ai",
        content="hi",
        provider="openai-chat",
        provider_raw={"object": "chat.completion", "id": "cc_1"},
        reasoning="x",
    )
    assert m.provider == "openai-chat"
    assert m.provider_raw["id"] == "cc_1"
