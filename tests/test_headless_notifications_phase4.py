"""Tests for Phase 4 of headless persistent sessions: magic-link tokens,
email dispatch, dedup + rate limiting. The DB round-trip (real INSERT +
SELECT + CAS UPDATE) is covered in integration tests; here we mock the
postgres pool and exercise the service module's contract surface.
"""

import hashlib
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

import orchestrator.services.headless_notifications as hn


# ---------------------------------------------------------------------------
# Helper: a fake db.acquire() context manager backed by a recordable mock conn
# ---------------------------------------------------------------------------


def _make_db(**overrides):
    """Build a MagicMock db with .acquire() yielding a mock connection
    whose methods can be stubbed via overrides.

    overrides: dict of attr-name → coroutine return value or AsyncMock.
    """
    fake_conn = MagicMock()
    for attr in ("fetchval", "fetchrow", "execute", "fetch"):
        if attr in overrides:
            val = overrides[attr]
            if callable(val) and not isinstance(val, AsyncMock):
                setattr(fake_conn, attr, AsyncMock(side_effect=val))
            else:
                setattr(fake_conn, attr, AsyncMock(return_value=val))
        else:
            setattr(fake_conn, attr, AsyncMock(return_value=None))

    class _Acquire:
        async def __aenter__(self):
            return fake_conn

        async def __aexit__(self, exc_type, exc, tb):
            return None

    db = MagicMock()
    db.acquire = lambda: _Acquire()
    db._fake_conn = fake_conn
    return db


# ---------------------------------------------------------------------------
# Section 1 — Token generation and validation primitives
# ---------------------------------------------------------------------------


class TestHashToken:
    def test_returns_sha256_hex(self):
        h = hn._hash_token("hello")
        # Deterministic SHA-256("hello").hexdigest()
        expected = hashlib.sha256(b"hello").hexdigest()
        assert h == expected

    def test_different_inputs_different_hashes(self):
        assert hn._hash_token("a") != hn._hash_token("b")


class TestGenerateMagicLinkToken:
    @pytest.mark.asyncio
    async def test_inserts_hash_not_raw(self):
        """The DB only ever stores SHA-256(token), never the raw token."""
        db = _make_db(fetchval="row-uuid")
        raw, row_id = await hn.generate_magic_link_token(
            db,
            purpose="approve_permission",
            user_id="user-1",
            approval_id="approval-1",
            thread_id="thread-1",
            intended_decision="approved",
        )
        assert row_id == "row-uuid"
        # The INSERT bound $1 to the *hash* of the raw token, not the raw.
        bound_args = db._fake_conn.fetchval.await_args.args
        sql = bound_args[0]
        assert "INSERT INTO magic_link_tokens" in sql
        bound_token_hash = bound_args[1]
        assert bound_token_hash == hn._hash_token(raw)
        assert bound_token_hash != raw

    @pytest.mark.asyncio
    async def test_rejects_invalid_decision(self):
        db = _make_db(fetchval="row-uuid")
        with pytest.raises(ValueError):
            await hn.generate_magic_link_token(
                db,
                purpose="approve_permission",
                user_id="u",
                approval_id="a",
                thread_id="t",
                intended_decision="maybe",
            )

    @pytest.mark.asyncio
    async def test_returns_url_safe_token(self):
        """secrets.token_urlsafe is base64-url — no padding, only safe chars."""
        db = _make_db(fetchval="x")
        raw, _ = await hn.generate_magic_link_token(
            db,
            purpose="approve_permission",
            user_id=None,
            approval_id=None,
            thread_id=None,
        )
        # url-safe charset: ASCII letters, digits, -, _.
        assert all(c.isalnum() or c in "-_" for c in raw)


class TestValidateMagicLink:
    @pytest.mark.asyncio
    async def test_returns_none_when_token_missing(self):
        db = _make_db(fetchrow=None)
        result = await hn.validate_magic_link(db, "anything")
        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_for_empty_token(self):
        db = _make_db()
        result = await hn.validate_magic_link(db, "")
        assert result is None
        db._fake_conn.fetchrow.assert_not_called()

    @pytest.mark.asyncio
    async def test_returns_none_when_already_used(self):
        db = _make_db(
            fetchrow={
                "id": "x",
                "purpose": "approve_permission",
                "user_id": "u",
                "approval_id": "a",
                "thread_id": "t",
                "intended_decision": "approved",
                "expires_at": datetime.now(timezone.utc) + timedelta(hours=1),
                "used_at": datetime.now(timezone.utc),  # already used
                "consumed_decision": "approved",
            }
        )
        result = await hn.validate_magic_link(db, "raw")
        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_when_expired(self):
        db = _make_db(
            fetchrow={
                "id": "x",
                "purpose": "approve_permission",
                "user_id": "u",
                "approval_id": "a",
                "thread_id": "t",
                "intended_decision": "approved",
                "expires_at": datetime.now(timezone.utc)
                - timedelta(minutes=1),  # expired
                "used_at": None,
                "consumed_decision": None,
            }
        )
        result = await hn.validate_magic_link(db, "raw")
        assert result is None

    @pytest.mark.asyncio
    async def test_returns_row_when_valid(self):
        db = _make_db(
            fetchrow={
                "id": "x",
                "purpose": "approve_permission",
                "user_id": "u",
                "approval_id": "a",
                "thread_id": "t",
                "intended_decision": "approved",
                "expires_at": datetime.now(timezone.utc) + timedelta(hours=1),
                "used_at": None,
                "consumed_decision": None,
            }
        )
        result = await hn.validate_magic_link(db, "raw")
        assert result is not None
        assert result["id"] == "x"
        assert result["intended_decision"] == "approved"

    @pytest.mark.asyncio
    async def test_looks_up_by_hash_not_raw(self):
        """The SELECT binds the hash of the raw token, never the raw."""
        db = _make_db(fetchrow=None)
        await hn.validate_magic_link(db, "my-secret-token")
        bound = db._fake_conn.fetchrow.await_args.args
        assert bound[1] == hn._hash_token("my-secret-token")


class TestConsumeMagicLink:
    @pytest.mark.asyncio
    async def test_returns_row_on_first_consume(self):
        db = _make_db(
            fetchrow={
                "id": "x",
                "purpose": "approve_permission",
                "user_id": "u",
                "approval_id": "a",
                "thread_id": "t",
                "intended_decision": "approved",
                "consumed_decision": "approved",
            }
        )
        result = await hn.consume_magic_link(db, "x", "approved")
        assert result is not None
        assert result["consumed_decision"] == "approved"
        # The UPDATE binds WHERE used_at IS NULL (single-use CAS).
        sql = db._fake_conn.fetchrow.await_args.args[0]
        assert "WHERE id = $1 AND used_at IS NULL" in sql

    @pytest.mark.asyncio
    async def test_returns_none_on_double_consume(self):
        """CAS lost (used_at already set) → fetchrow returns None."""
        db = _make_db(fetchrow=None)
        result = await hn.consume_magic_link(db, "x", "approved")
        assert result is None

    @pytest.mark.asyncio
    async def test_rejects_invalid_decision(self):
        db = _make_db()
        result = await hn.consume_magic_link(db, "x", "maybe")
        assert result is None
        db._fake_conn.fetchrow.assert_not_called()


# ---------------------------------------------------------------------------
# Section 2 — Rate-limit probe
# ---------------------------------------------------------------------------
#
# The standalone dedup probe (`already_notified`) was retired 2026-05-13 —
# the sweeper SQL is now the authoritative dedup, and an in-process probe
# only duplicated logic that could drift apart. See
# orchestrator/services/session_attention.py:thread_permission_notify_sweeper
# for the widened
# IN-set that absorbed the probe's responsibilities.


# ---------------------------------------------------------------------------
# Section 3 — record_permission_pending (the feed row with the magic links)
# ---------------------------------------------------------------------------


class _Recorded:
    def __init__(self, inserted=True):
        self.notification_id = "n-1"
        self.inserted = inserted
        self.deliveries = {"in_app": True}


class TestRecordPermissionPending:
    def _row(self, **overrides):
        row = {
            "id": "a",
            "thread_id": "t",
            "tool_name": "run_command",
            "tool_args": {"cmd": "ls"},
            "requested_at": datetime.now(timezone.utc) - timedelta(minutes=3),
            "user_id": "u",
            "title": "Nightly build",
        }
        row.update(overrides)
        return row

    @pytest.mark.asyncio
    async def test_records_a_high_row_with_both_magic_links(self, monkeypatch):
        db = _make_db(execute="INSERT 0 1")
        tokens = iter(["approve-tok", "deny-tok"])

        async def _gen(db_, **kw):
            return next(tokens), "hashed"

        monkeypatch.setattr(hn, "generate_magic_link_token", _gen)
        notifier = MagicMock()
        notifier.record = AsyncMock(return_value=_Recorded())

        result = await hn.record_permission_pending(
            db, notifier, row=self._row(), cockpit_external_url="http://x/"
        )
        assert result == {"status": "recorded", "notification_id": "n-1"}
        kw = notifier.record.await_args.kwargs
        assert kw["recipient_id"] == "u"
        assert kw["category"] == "session_permission"
        assert kw["dedup_key"] == "session_permission:a"
        assert (kw["source_kind"], kw["source_id"]) == ("permission_request", "a")
        assert kw["action_params"] == {"thread_id": "t", "request_id": "a"}
        assert kw["subject"] == "Approval needed: run_command"
        body = kw["body"]
        assert "Approve: http://x/magic/approve/approve-tok" in body
        assert "Deny: http://x/magic/approve/deny-tok" in body
        assert "http://x/sessions/t" in body
        assert "Nightly build" in body and "3 min ago" in body
        assert '"cmd": "ls"' in body
        assert kw["payload"]["tool_name"] == "run_command"
        assert kw["payload"]["request_id"] == "a"

    @pytest.mark.asyncio
    async def test_replay_reports_replayed(self, monkeypatch):
        db = _make_db()

        async def _gen(db_, **kw):
            return "tok", "hashed"

        monkeypatch.setattr(hn, "generate_magic_link_token", _gen)
        notifier = MagicMock()
        notifier.record = AsyncMock(return_value=_Recorded(inserted=False))
        result = await hn.record_permission_pending(
            db, notifier, row=self._row(), cockpit_external_url="http://x"
        )
        assert result["status"] == "replayed"

    @pytest.mark.asyncio
    async def test_no_owner_means_nobody_to_notify(self):
        db = _make_db()
        notifier = MagicMock()
        notifier.record = AsyncMock()
        result = await hn.record_permission_pending(
            db, notifier, row=self._row(user_id=None), cockpit_external_url="http://x"
        )
        assert result == {"status": "skipped_no_owner"}
        notifier.record.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_string_tool_args_are_parsed_for_the_preview(self, monkeypatch):
        db = _make_db()

        async def _gen(db_, **kw):
            return "tok", "hashed"

        monkeypatch.setattr(hn, "generate_magic_link_token", _gen)
        notifier = MagicMock()
        notifier.record = AsyncMock(return_value=_Recorded())
        await hn.record_permission_pending(
            db,
            notifier,
            row=self._row(tool_args='{"path": "/etc"}'),
            cockpit_external_url="http://x",
        )
        assert '"path": "/etc"' in notifier.record.await_args.kwargs["body"]


class TestBuildMagicLinkUrl:
    def test_appends_magic_approve_path(self):
        url = hn._build_magic_link_url("http://cockpit.example.com/", "raw-tok")
        assert url == "http://cockpit.example.com/magic/approve/raw-tok"

    def test_url_encodes_special_chars_in_token(self):
        url = hn._build_magic_link_url("http://x", "tok with space")
        assert "%20" in url or "+" in url
        # Must not contain a raw space.
        assert " " not in url

    def test_strips_trailing_slash_from_base(self):
        url = hn._build_magic_link_url("http://x///", "tok")
        assert url.startswith("http://x/magic/approve/")
        assert "////" not in url


class TestTruncateArgsForEmail:
    def test_short_args_unchanged(self):
        out = hn._truncate_args_for_email({"a": 1})
        assert "1" in out
        assert "truncated" not in out

    def test_long_args_truncated_with_marker(self):
        out = hn._truncate_args_for_email({"x": "a" * 5000})
        assert "truncated" in out
        assert len(out) < 5000

    def test_handles_unserializable_input(self):
        class _NotJsonable:
            def __repr__(self):
                return "<NotJsonable>"

        # No exception; falls back to str() (json.dumps with default=str
        # handles this — but if the type explodes, we still get something).
        out = hn._truncate_args_for_email({"x": _NotJsonable()})
        assert isinstance(out, str)


# ---------------------------------------------------------------------------
# Section 6 — Sweeper dedup SQL contract
# ---------------------------------------------------------------------------
#
# Captures the SELECT the permission-notify sweeper builds and asserts on
# its shape. Live DB round-trips are covered by the smoke runbook; here we
# pin the SQL contract so regressions on the IN-set or recency floor are
# caught at unit-test time.


def _notify_sweeper_dependencies(store, *, notification_service=None, cockpit_url=None):
    """The permission-notify sweeper's application collaborators.

    Only the store, the notification service and the cockpit URL are read by
    this sweeper; the rest are inert placeholders.
    """
    from orchestrator.services import session_attention

    return session_attention.SessionAttentionDependencies(
        store=store,
        container_provisioner=MagicMock(),
        workspace_suspension=MagicMock(),
        persistent_provisioner=None,
        persistent_thread_recycler=lambda: None,
        emit_session_provisioning_failure=AsyncMock(),
        thread_retirement_operations=MagicMock(),
        notification_service=(
            notification_service if notification_service is not None else MagicMock()
        ),
        cockpit_url=cockpit_url or (lambda: "http://localhost:4200"),
    )


class TestPermissionNotifySweeperSQL:
    @pytest.mark.asyncio
    async def test_selects_aged_pending_gates_without_a_feed_row(self, monkeypatch):
        import asyncio

        from orchestrator.services import session_attention

        captured: dict = {}
        evt = asyncio.Event()

        async def _fake_fetch(query: str, *args):
            captured["query"] = query
            captured["args"] = args
            # Signal shutdown so the sweeper's wait_for exits and the
            # loop terminates cleanly after this first fetch.
            evt.set()
            return []

        fake_conn = MagicMock()
        fake_conn.fetch = _fake_fetch

        class _Acquire:
            async def __aenter__(self_inner):
                return fake_conn

            async def __aexit__(self_inner, exc_type, exc, tb):
                return None

        # The store is the sweeper's injected dependency, not a module global.
        fake_db = MagicMock()
        fake_db.acquire = lambda: _Acquire()

        await session_attention.thread_permission_notify_sweeper(
            evt, dependencies=_notify_sweeper_dependencies(fake_db)
        )

        q = captured.get("query", "")
        # Only gates still pending and older than the age threshold …
        assert "r.status = 'pending'" in q
        assert "$1::int * interval '1 second'" in q
        # … that have no feed row yet (the row is the dedup; tokens are
        # minted once). The thread join supplies the recipient.
        assert "n.source_kind = 'permission_request'" in q
        assert "n.source_id = r.id::text" in q
        assert "JOIN threads t ON t.id = r.thread_id" in q
        assert "thread_notifications" not in q
        args = captured.get("args", ())
        assert args == (30,)  # HEADLESS_NOTIFY_AGE_S default

    @pytest.mark.asyncio
    async def test_records_every_aged_gate_through_the_feed(self, monkeypatch):
        """Each selected row becomes one ``record_permission_pending`` call on
        the application's store and notifier, with the cockpit URL resolved
        once per sweep. A failing row is logged and the next is still
        recorded (best-effort)."""
        import asyncio

        from orchestrator.services import session_attention

        evt = asyncio.Event()
        rows = [
            {"id": "req-1", "thread_id": "thread-1", "tool_name": "a"},
            {"id": "req-2", "thread_id": "thread-2", "tool_name": "b"},
        ]

        async def _fake_fetch(query: str, *args):
            evt.set()
            return rows

        fake_conn = MagicMock()
        fake_conn.fetch = _fake_fetch

        class _Acquire:
            async def __aenter__(self_inner):
                return fake_conn

            async def __aexit__(self_inner, exc_type, exc, tb):
                return None

        fake_db = MagicMock()
        fake_db.acquire = lambda: _Acquire()
        notifier = MagicMock()
        record = AsyncMock(
            side_effect=[RuntimeError("smtp down"), {"status": "recorded"}]
        )
        # ``headless_notifications`` is looked up as a module-level name in
        # ``session_attention`` at call time.
        monkeypatch.setattr(
            session_attention,
            "headless_notifications",
            MagicMock(record_permission_pending=record),
        )

        await session_attention.thread_permission_notify_sweeper(
            evt,
            dependencies=_notify_sweeper_dependencies(
                fake_db,
                notification_service=notifier,
                cockpit_url=lambda: "https://cockpit.test",
            ),
        )

        assert [call.kwargs["row"] for call in record.await_args_list] == rows
        for call in record.await_args_list:
            assert call.args == (fake_db, notifier)
            assert call.kwargs["cockpit_external_url"] == "https://cockpit.test"
