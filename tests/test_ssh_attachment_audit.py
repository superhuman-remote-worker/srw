# tests/test_ssh_attachment_audit.py
import asyncio
import logging
import pathlib
import re
from unittest.mock import MagicMock
from uuid import UUID

import pytest

from orchestrator.database.postgres import PostgresDB

MIGRATIONS = (
    pathlib.Path(__file__).resolve().parents[1]
    / "src"
    / "orchestrator"
    / "database"
    / "migrations"
    / "app"
)
SQL = MIGRATIONS / "0204_ssh_attachments.sql"


def test_migration_exists():
    assert SQL.exists()


def test_records_the_fields_the_existing_trail_cannot():
    """agent_audit has no user_id, client_ip or thread_id, which is exactly why
    this table exists."""
    body = SQL.read_text()
    for column in (
        "thread_id",
        "user_id",
        "ssh_key_id",
        "client_ip",
        "attached_at",
        "detached_at",
        "channels",
    ):
        assert column in body, f"missing column {column}"


def test_survives_key_deletion():
    """Revoking a key must not erase the record of what it was used for.

    Pinned as one clause, not two independent substring checks: user_id also
    carries ON DELETE SET NULL, so checking "ssh_key_id" and "ON DELETE SET
    NULL" separately would still pass if ssh_key_id alone were switched to
    CASCADE.
    """
    assert re.search(
        r"ssh_key_id\s+uuid\s+REFERENCES public\.user_ssh_keys\(id\)\s+"
        r"ON DELETE SET NULL",
        SQL.read_text(),
    )


def test_survives_session_deletion():
    """Ending a session must not erase the record that it was SSH'd into.

    An audit trail the audited party can delete on demand (by ending its own
    session) is not an audit trail -- thread_id carries ON DELETE SET NULL,
    not CASCADE, and the owner decision this migration now encodes says that
    row must outlive the thread it describes.

    Pinned as one clause, not two independent substring checks, for the same
    reason as test_survives_key_deletion: "thread_id" and "ON DELETE SET
    NULL" checked separately would still pass if thread_id alone carried
    CASCADE while some other column happened to carry SET NULL. The pattern
    also doubles as proof thread_id is no longer NOT NULL -- "uuid NOT NULL
    REFERENCES" would not match "uuid\\s+REFERENCES".
    """
    assert re.search(
        r"thread_id\s+uuid\s+REFERENCES public\.threads\(id\)\s+"
        r"ON DELETE SET NULL",
        SQL.read_text(),
    )


def test_no_cascade_deletes():
    """All three FKs -- user_id, ssh_key_id and now thread_id -- are SET
    NULL. Growth is bounded by the retention sweeper instead of cascade,
    matching how 0025_security_events.sql treats security audit."""
    assert "ON DELETE CASCADE" not in SQL.read_text()


class FakeConn:
    def __init__(self, fetchval=None, execute="UPDATE 1"):
        self._fetchval = fetchval
        self._execute = execute
        self.calls = []
        # Count of times a connection was actually checked out, separate from
        # `calls`: proves "no connection taken", not just "no statement
        # issued" — the two are different claims (see
        # test_close_rejects_a_malformed_attachment_id).
        self.acquired = 0

    async def fetchval(self, sql, *args):
        self.calls.append((sql, args))
        return self._fetchval

    async def execute(self, sql, *args):
        self.calls.append((sql, args))
        return self._execute


class FakeAcquire:
    def __init__(self, conn):
        self.conn = conn

    async def __aenter__(self):
        self.conn.acquired += 1
        return self.conn

    async def __aexit__(self, *exc):
        return False


def _db(conn):
    db = PostgresDB.__new__(PostgresDB)
    db.acquire = lambda: FakeAcquire(conn)
    return db


@pytest.mark.asyncio
async def test_record_returns_an_id():
    thread_id = "00000000-0000-0000-0000-000000000002"
    conn = FakeConn(fetchval=UUID("00000000-0000-0000-0000-0000000000a1"))
    got = await _db(conn).record_ssh_attachment(
        thread_id=thread_id,
        user_id="00000000-0000-0000-0000-000000000001",
        ssh_key_id="00000000-0000-0000-0000-000000000003",
        client_ip="203.0.113.7",
        handle="s-7f3a91c2",
    )
    assert got == "00000000-0000-0000-0000-0000000000a1"
    # Bound as a UUID, not the raw string — same contract as
    # close_ssh_attachment. Pinned here too so deleting the UUID() wrap on
    # either method fails a test, not just one of them.
    assert conn.calls[0][1][0] == UUID(thread_id)


@pytest.mark.asyncio
async def test_close_sets_detached_at_and_channels():
    conn = FakeConn()
    attachment_id = "00000000-0000-0000-0000-0000000000a1"
    await _db(conn).close_ssh_attachment(attachment_id, ["session", "sftp"])
    sql, args = conn.calls[0]
    assert "detached_at" in sql
    assert "session" in args[1]
    # Bound as a UUID, not the raw string, so a malformed id fails here rather
    # than reaching the driver — same contract as record_ssh_attachment.
    assert args[0] == UUID(attachment_id)


@pytest.mark.asyncio
async def test_close_reports_one_row_for_a_normal_close():
    """``WHERE detached_at IS NULL`` makes an already-closed or unknown id a
    silent no-op, so the row count is the only thing that tells the gateway's
    detach handler "closed" from "nothing happened"."""
    conn = FakeConn(execute="UPDATE 1")
    assert (
        await _db(conn).close_ssh_attachment(
            "00000000-0000-0000-0000-0000000000a1", ["session"]
        )
        == 1
    )


@pytest.mark.asyncio
async def test_close_reports_zero_for_an_already_closed_or_unknown_id():
    conn = FakeConn(execute="UPDATE 0")
    assert (
        await _db(conn).close_ssh_attachment(
            "00000000-0000-0000-0000-0000000000a1", ["session"]
        )
        == 0
    )


@pytest.mark.asyncio
async def test_close_rejects_a_malformed_attachment_id():
    conn = FakeConn()
    with pytest.raises(ValueError):
        await _db(conn).close_ssh_attachment("a1", ["session"])
    assert conn.calls == []
    # conn.calls == [] only proves no statement was issued. The id is parsed
    # before self.acquire() runs, so a malformed id never checks a connection
    # out of the pool either -- assert that claim directly, not just its
    # weaker cousin.
    assert conn.acquired == 0


# =============================================================================
# get_ssh_attachment_thread_id -- fix round 2: lets the close endpoint
# authorize a close against the attachment's own thread instead of trusting
# an unauthenticated X-Internal-Key holder with an opaque UUID.
# =============================================================================


@pytest.mark.asyncio
async def test_get_thread_id_returns_the_attachments_thread_id():
    thread_id = UUID("00000000-0000-0000-0000-000000000002")
    conn = FakeConn(fetchval=thread_id)
    got = await _db(conn).get_ssh_attachment_thread_id(
        "00000000-0000-0000-0000-0000000000a1"
    )
    assert got == str(thread_id)
    # Bound as a UUID, not the raw string -- same contract as every other
    # method on this table.
    assert conn.calls[0][1][0] == UUID("00000000-0000-0000-0000-0000000000a1")


@pytest.mark.asyncio
async def test_get_thread_id_returns_none_for_an_unknown_attachment_id():
    conn = FakeConn(fetchval=None)
    got = await _db(conn).get_ssh_attachment_thread_id(
        "00000000-0000-0000-0000-0000000000a1"
    )
    assert got is None


@pytest.mark.asyncio
async def test_get_thread_id_returns_none_when_the_thread_was_deleted():
    """thread_id carries ON DELETE SET NULL (test_survives_session_deletion
    above), so a real attachment row whose thread no longer exists reads
    back NULL here. SELECT ... WHERE id = $1 cannot tell that apart from "no
    row matched" -- fetchval returns None either way -- and that collapse is
    intentional, not a gap: there is no thread left to authorize a close
    against, so the close endpoint must refuse it exactly like an unknown
    attachment id. The row's detached_at then stays NULL forever in this
    (rare: a live channel outliving its own thread's deletion) case, a
    data-quality nit traded for never resurrecting an access check with no
    thread on the other end of it.
    """
    conn = FakeConn(fetchval=None)
    got = await _db(conn).get_ssh_attachment_thread_id(
        "00000000-0000-0000-0000-0000000000a1"
    )
    assert got is None


@pytest.mark.asyncio
async def test_get_thread_id_rejects_a_malformed_attachment_id():
    conn = FakeConn()
    with pytest.raises(ValueError):
        await _db(conn).get_ssh_attachment_thread_id("a1")
    assert conn.calls == []
    assert conn.acquired == 0


@pytest.mark.asyncio
async def test_record_rejects_a_malformed_thread_id_before_acquiring():
    conn = FakeConn()
    with pytest.raises(ValueError):
        await _db(conn).record_ssh_attachment(
            thread_id="not-a-uuid",
            user_id="00000000-0000-0000-0000-000000000001",
            ssh_key_id=None,
            client_ip="203.0.113.7",
            handle="s-7f3a91c2",
        )
    assert conn.calls == []
    assert conn.acquired == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "handle",
    [
        "s-abc\nProxyCommand x",  # config injection, the charset's whole point
        "s-TOOLONGHANDLE",
        "s-abcdefgi",  # 'i' is not in the Crockford-minus-ambiguous alphabet
        "",
        "root",
    ],
)
async def test_record_rejects_a_malformed_handle_before_acquiring(handle):
    """``handle`` lands in a ``NOT NULL text`` column with no CHECK behind it.

    The charset is a security boundary (it is written verbatim into a user's
    ``~/.ssh/config``) and ``get_thread_id_by_ssh_handle`` already guards it
    defensively rather than trusting callers — on the stated grounds that the
    boundary must not depend on every future caller remembering to
    pre-validate. An audit row is exactly where a malformed handle would be
    believed later.
    """
    conn = FakeConn()
    with pytest.raises(ValueError):
        await _db(conn).record_ssh_attachment(
            thread_id="00000000-0000-0000-0000-000000000002",
            user_id="00000000-0000-0000-0000-000000000001",
            ssh_key_id=None,
            client_ip="203.0.113.7",
            handle=handle,
        )
    assert conn.calls == []
    assert conn.acquired == 0


@pytest.mark.asyncio
async def test_record_nulls_a_socket_path_client_ip():
    """A unix-socket peer has no parseable address. The audit row must still
    be written -- a row missing client_ip beats no row at all."""
    conn = FakeConn(fetchval=UUID("00000000-0000-0000-0000-0000000000a1"))
    await _db(conn).record_ssh_attachment(
        thread_id="00000000-0000-0000-0000-000000000002",
        user_id="00000000-0000-0000-0000-000000000001",
        ssh_key_id=None,
        client_ip="/run/ssh.sock",
        handle="s-7f3a91c2",
    )
    assert conn.calls[0][1][4] is None


@pytest.mark.asyncio
async def test_record_nulls_an_empty_client_ip():
    conn = FakeConn(fetchval=UUID("00000000-0000-0000-0000-0000000000a1"))
    await _db(conn).record_ssh_attachment(
        thread_id="00000000-0000-0000-0000-000000000002",
        user_id="00000000-0000-0000-0000-000000000001",
        ssh_key_id=None,
        client_ip="",
        handle="s-7f3a91c2",
    )
    assert conn.calls[0][1][4] is None


@pytest.mark.asyncio
async def test_record_keeps_a_valid_client_ip():
    conn = FakeConn(fetchval=UUID("00000000-0000-0000-0000-0000000000a1"))
    await _db(conn).record_ssh_attachment(
        thread_id="00000000-0000-0000-0000-000000000002",
        user_id="00000000-0000-0000-0000-000000000001",
        ssh_key_id=None,
        client_ip="203.0.113.7",
        handle="s-7f3a91c2",
    )
    assert conn.calls[0][1][4] == "203.0.113.7"


# =============================================================================
# prune_ssh_attachments -- retention sweep, mirroring prune_security_events
# =============================================================================


@pytest.mark.asyncio
async def test_prune_deletes_rows_older_than_retention():
    conn = FakeConn(fetchval=7)
    got = await _db(conn).prune_ssh_attachments(retention_days=45)
    assert got == 7
    sql, args = conn.calls[0]
    assert "DELETE FROM ssh_attachments" in sql
    assert "attached_at" in sql
    # Bound as a parameter, not interpolated into the SQL text.
    assert args == (45,)


@pytest.mark.asyncio
async def test_prune_defaults_to_ninety_days():
    conn = FakeConn(fetchval=0)
    await _db(conn).prune_ssh_attachments()
    assert conn.calls[0][1] == (90,)


@pytest.mark.asyncio
async def test_prune_returns_zero_when_fetchval_is_none():
    """Mirrors prune_security_events's ``int(count or 0)`` guard."""
    conn = FakeConn(fetchval=None)
    assert await _db(conn).prune_ssh_attachments() == 0


# =============================================================================
# ssh_attachments_prune_sweeper -- env-var defaults
# =============================================================================
#
# The loop body (the actual prune call) is exercised above through
# PostgresDB.prune_ssh_attachments directly, and end to end against real
# PostgreSQL in test_b11_periodic_loops.py. What's cheap to prove here is the
# sweeper's env-var default resolution: pre-setting shutdown_event before the
# call makes ``while not shutdown_event.is_set()`` false on its first check, so
# the loop body -- and the only access to the injected store -- never
# executes. Only the two log lines bracketing it fire, and the first one names
# the defaults it resolved. (R1.B11 moved the sweeper out of main into
# services/retention_sweepers.py; it takes the store as a keyword parameter.)


@pytest.mark.asyncio
async def test_sweeper_logs_default_interval_and_retention(caplog, monkeypatch):
    monkeypatch.delenv("SSH_ATTACHMENTS_PRUNE_INTERVAL_S", raising=False)
    monkeypatch.delenv("SSH_ATTACHMENTS_RETENTION_DAYS", raising=False)
    from orchestrator.services.retention_sweepers import (
        ssh_attachments_prune_sweeper,
    )

    store = MagicMock()
    shutdown_event = asyncio.Event()
    shutdown_event.set()
    with caplog.at_level(logging.INFO):
        await ssh_attachments_prune_sweeper(shutdown_event, store=store)
    assert store.mock_calls == []
    assert "interval=3600" in caplog.text
    assert "retention=90" in caplog.text


@pytest.mark.asyncio
async def test_sweeper_honors_env_var_overrides(caplog, monkeypatch):
    monkeypatch.setenv("SSH_ATTACHMENTS_PRUNE_INTERVAL_S", "120")
    monkeypatch.setenv("SSH_ATTACHMENTS_RETENTION_DAYS", "14")
    from orchestrator.services.retention_sweepers import (
        ssh_attachments_prune_sweeper,
    )

    store = MagicMock()
    shutdown_event = asyncio.Event()
    shutdown_event.set()
    with caplog.at_level(logging.INFO):
        await ssh_attachments_prune_sweeper(shutdown_event, store=store)
    assert store.mock_calls == []
    assert "interval=120" in caplog.text
    assert "retention=14" in caplog.text
