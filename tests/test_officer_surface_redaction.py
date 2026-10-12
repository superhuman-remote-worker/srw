"""The remaining officer/user presentation surfaces run the OC-05 sanitizer.

knowledge-base/knowledge/issues/officer_evidence_and_messages_leak_secret_shaped_content.md:
evidence reads and escalated worker bodies were already sanitized; the raw
completion-report endpoint, the SITREP / wake excerpts, the officer's
escalation context and outbound notification bodies were not. A worker (or a
prompt-injected page it summarized, or a failed push echoing its remote) can
put a credential into any of them.

Each surface is pinned both ways: the synthetic secret does not survive, and
clean text is byte-identical to what it was before redaction existed.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from orchestrator.database.postgres import JobQueryResult
from orchestrator.services import job_artifacts, session_wake, sitrep
from orchestrator.services.message_routing import _delimited_user_body
from orchestrator.services.notification_service import NotificationService
from shared.content_redaction import REDACTED, sanitize, sanitize_data
from shared.orch_surface.formatters import (
    format_completion_report,
    format_evidence_read,
)

# Synthetic, never real.
TOKEN = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b"
REMOTE = f"https://oauth2:{TOKEN}@gitea.local/org/repo.git"
API_KEY = "sk-abcdefghijklmnopqrstuvwxyz012345"
PROJECT_ID = str(uuid.uuid4())
THREAD_ID = str(uuid.uuid4())
USER = "11111111-1111-1111-1111-111111111111"


def _assert_clean(text: str) -> None:
    for secret in (TOKEN, API_KEY, "hunter2"):
        assert secret not in text, secret


# =============================================================================
# sanitize_data — the structured form the completion report needs
# =============================================================================


class TestSanitizeData:
    def test_every_string_is_sanitized_and_the_count_aggregates(self):
        report = {
            "summary": f"pushed via {REMOTE}",
            "deliverables": ["out/report.md", {"note": "password: hunter2"}],
            "confidence": 0.8,
            "notes": None,
        }
        clean = sanitize_data(report)
        _assert_clean(json.dumps(clean.value))
        assert clean.count == 2 and clean.redacted
        assert clean.value["deliverables"][0] == "out/report.md"
        assert clean.value["confidence"] == 0.8 and clean.value["notes"] is None

    def test_the_result_stays_valid_json_where_text_redaction_would_not(self):
        # Sanitizing the SERIALIZED form eats the backslash of an escaped
        # quote and ends the string early.
        report = {"summary": 'token=abc\\"def'}
        with pytest.raises(json.JSONDecodeError):
            json.loads(sanitize(json.dumps(report)).text)
        json.loads(json.dumps(sanitize_data(report).value))

    def test_clean_data_is_unchanged(self):
        report = {"summary": "all green", "deliverables": ["a.md"]}
        clean = sanitize_data(report)
        assert clean.value == report and not clean.redacted

    def test_a_secret_named_key_agrees_with_the_serialized_view(self):
        # The evidence page sanitizes the serialized report, where
        # `"password": "hunter2"` is a key-value match. Parsed apart, the
        # value alone says nothing — the key must carry the verdict.
        report = {
            "deliverables": [
                {"path": "out/a.md", "password": "hunter2", "github.token": "t0k"},
                {"max_tokens": 4096, "tokenizer": "bert", "token": "${TOKEN}"},
            ]
        }
        clean = sanitize_data(report)
        serialized = sanitize(json.dumps(report)).text
        first, second = clean.value["deliverables"]
        assert first == {
            "path": "out/a.md",
            "password": REDACTED,
            "github.token": REDACTED,
        }
        assert second == report["deliverables"][1]
        assert "hunter2" not in serialized and "t0k" not in serialized
        assert clean.count == 2


# =============================================================================
# Raw completion report (GET /api/jobs/{id}/completion-report)
# =============================================================================


def _report_dependencies(report: dict) -> SimpleNamespace:
    manifest = {
        "recorded_at": "2026-09-22T00:00:00+00:00",
        "entries": [
            {
                "kind": "completion_report",
                "inline_content": json.dumps(report, indent=2),
                "source": {"type": "inline", "revision": "a" * 40},
            }
        ],
    }
    return SimpleNamespace(
        evidence=SimpleNamespace(parse_manifest=lambda _job: manifest)
    )


async def _completion_report(report: dict) -> dict:
    return await job_artifacts.get_job_completion_report_route(
        job_id="job-1",
        authorized_job={"id": "job-1"},
        dependencies=_report_dependencies(report),
    )


class TestCompletionReport:
    @pytest.mark.asyncio
    async def test_worker_claims_are_sanitized_with_a_count(self):
        body = await _completion_report(
            {
                "summary": f"Pushed to {REMOTE}; set OPENAI key {API_KEY}",
                "deliverables": [{"path": "out/r.md", "note": "password: hunter2"}],
                "confidence": 0.9,
                "notes": "done",
            }
        )
        _assert_clean(json.dumps(body))
        assert body["redacted"] is True and body["redacted_count"] == 3
        # What the officer navigates by survives.
        assert "gitea.local/org/repo.git" in body["report"]["summary"]
        assert body["report"]["deliverables"][0]["path"] == "out/r.md"
        assert body["report"]["confidence"] == 0.9

    @pytest.mark.asyncio
    async def test_a_clean_report_is_returned_exactly_as_before(self):
        report = {
            "summary": "Wrote report for commit 9e4c8d63a1",
            "deliverables": ["out/report.md"],
            "confidence": 0.7,
            "notes": None,
            "reported_at": "2026-09-22T00:00:00Z",
        }
        body = await _completion_report(report)
        assert body["report"] == report
        assert "redacted" not in body and "redacted_count" not in body

    def test_the_officer_text_says_values_were_withheld(self):
        text = format_completion_report(
            "job-1",
            {
                "report": {"summary": f"pushed via https://{REDACTED}@h/x.git"},
                "redacted": True,
                "redacted_count": 3,
            },
        )
        assert "3 secret-shaped value(s) redacted" in text
        assert "redacted from" not in format_completion_report(
            "job-1", {"report": {"summary": "ok"}}
        )

    def test_the_evidence_page_text_says_so_too(self):
        # The API always returned redacted/redacted_count; the text view the
        # officer actually reads dropped it.
        text = format_evidence_read(
            "job-1",
            "ev_1",
            {
                "entry": {},
                "content": f"password: {REDACTED}",
                "offset": 0,
                "total_chars": 20,
                "redacted": True,
                "redacted_count": 1,
            },
        )
        assert "1 secret-shaped value(s) redacted" in text


# =============================================================================
# SITREP / inbox excerpts and the wake renderers
# =============================================================================


class _Acquire:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *args):
        return False


class TestSitrepExcerpts:
    def test_wake_reason_summary_is_sanitized(self):
        lines = sitrep._reason_lines(
            [
                {
                    "source": "job_transition",
                    "dedup_key": "job:1:failed",
                    "payload": {"summary": f"push failed: {REMOTE}"},
                }
            ]
        )
        text = "\n".join(lines)
        _assert_clean(text)
        assert "gitea.local/org/repo.git" in text

    def test_redaction_runs_before_the_cut(self):
        # A 160-char cut landing inside the token would otherwise strand a
        # fragment that no longer matches any pattern.
        detail = "x" * 120 + f" {REMOTE}"
        text = "\n".join(
            sitrep._reason_lines(
                [{"source": "e", "dedup_key": "k", "payload": {"summary": detail}}]
            )
        )
        assert TOKEN[:10] not in text

    @pytest.mark.asyncio
    async def test_worker_message_subjects_in_the_inbox_are_sanitized(self):
        db = SimpleNamespace(
            list_open_worker_message_routes=AsyncMock(
                return_value=[
                    {
                        "job_id": str(uuid.uuid4()),
                        "thread_id": "t-1",
                        "state": "pending_officer",
                        "created_at": datetime.now(timezone.utc),
                        "blocking": True,
                        "subject": f"Need {API_KEY} rotated?",
                    }
                ]
            )
        )
        lines = await sitrep._worker_messages_section(
            db, PROJECT_ID, datetime.now(timezone.utc)
        )
        _assert_clean("\n".join(lines))
        assert "rotated?" in "\n".join(lines)

    @pytest.mark.asyncio
    async def test_a_pending_sudo_command_is_sanitized(self):
        now = datetime.now(timezone.utc)
        conn = SimpleNamespace(
            fetch=AsyncMock(
                side_effect=[
                    [
                        {
                            "id": 1,
                            "job_id": uuid.uuid4(),
                            "command": f"git clone {REMOTE}",
                            "request_type": "sudo",
                            "expires_at": now + timedelta(minutes=5),
                        }
                    ],
                    [],
                ]
            )
        )
        db = SimpleNamespace(acquire=lambda: _Acquire(conn))
        lines = await sitrep._pending_section(db, THREAD_ID, PROJECT_ID, now)
        text = "\n".join(lines)
        _assert_clean(text)
        assert "git clone" in text

    @pytest.mark.asyncio
    async def test_a_job_error_in_the_transitions_is_sanitized(self):
        job_id = str(uuid.uuid4())
        db = SimpleNamespace(
            query_jobs=AsyncMock(
                return_value=JobQueryResult(
                    jobs=[
                        {
                            "id": job_id,
                            "status": "failed",
                            "description": "Ship the fix",
                            "error_message": f"fatal: unable to access '{REMOTE}/'",
                        }
                    ]
                )
            )
        )
        now = datetime.now(timezone.utc)
        lines, _ = await sitrep._jobs_section(
            db,
            None,
            PROJECT_ID,
            {job_id: {"status": "processing"}},
            now - timedelta(minutes=10),
            now,
        )
        text = "\n".join(lines)
        _assert_clean(text)
        assert "unable to access" in text and "Ship the fix" in text

    def test_clean_excerpts_are_unchanged(self):
        lines = sitrep._reason_lines(
            [
                {
                    "source": "job_transition",
                    "dedup_key": "job:1:completed",
                    "payload": {"summary": "completed commit 9e4c8d63a1"},
                }
            ]
        )
        assert (
            lines[-1]
            == "- job_transition: job:1:completed — completed commit 9e4c8d63a1"
        )

    def test_the_minimal_fallback_wake_is_sanitized_too(self):
        text = session_wake._format_officer_wake(
            [
                {
                    "source": "job_transition",
                    "dedup_key": "job:1:failed",
                    "payload": {"summary": f"push failed: {REMOTE}"},
                },
                {
                    "source": "timer",
                    "payload": {"minutes": 30, "reason": f"retry {API_KEY}"},
                },
            ]
        )
        _assert_clean(text)
        assert "push failed" in text

    @pytest.mark.asyncio
    async def test_a_job_finished_wake_sanitizes_summary_and_error(self, monkeypatch):
        row = {
            "id": str(uuid.uuid4()),
            "status": "failed",
            "description": "Ship it",
            "freeze_data": {"summary": f"pushed to {REMOTE}"},
            "error_message": f"token={API_KEY}",
        }
        monkeypatch.setattr(
            session_wake, "_agent_label", AsyncMock(return_value="config: dev")
        )
        monkeypatch.setattr(session_wake, "_sibling_line", AsyncMock(return_value=""))
        text = await session_wake._format_wake_message(MagicMock(), row, THREAD_ID)
        _assert_clean(text)
        assert "gitea.local/org/repo.git" in text and "Ship it" in text


# =============================================================================
# Escalation context
# =============================================================================


class TestEscalationContext:
    def test_officer_context_is_sanitized_with_its_own_count(self):
        body = _delimited_user_body(
            "Question",
            "Can I push?",
            reason_line="**Escalated.**",
            officer_context=f"The worker pasted {REMOTE} into its log.",
        )
        _assert_clean(body)
        assert "gitea.local/org/repo.git" in body
        assert "redacted from the officer's context" in body
        # The worker's own clean text carries no note.
        assert "from the worker's text" not in body

    def test_clean_context_renders_exactly_as_before(self):
        body = _delimited_user_body(
            "Question",
            "Can I push?",
            reason_line="**Escalated.**",
            officer_context="Two lines\nof context",
        )
        assert "> Two lines\n> of context" in body
        assert "redacted" not in body


# =============================================================================
# Notification bodies — redacted by each producer, where the untrusted part is
# known; record() itself leaves the server's text (magic links) alone.
# =============================================================================


def _service() -> NotificationService:
    svc = NotificationService.__new__(NotificationService)
    svc._available = True
    svc._db = MagicMock()
    svc._notification_feed = None
    svc._cockpit_url = "https://cockpit"
    svc._transports = {}
    svc._persist_notification = AsyncMock(
        side_effect=lambda row, steps=None: (row["id"], True)
    )
    svc._claim_delivery = AsyncMock(return_value="claim-1")
    svc._settle_delivery = AsyncMock()
    svc._record_suppressed = AsyncMock()
    svc._defer_steps = AsyncMock(return_value=1)
    svc._get_user = AsyncMock(
        return_value={"id": USER, "email": "legate@example.org", "display_name": "L"}
    )
    svc._get_user_channels = AsyncMock(return_value={"email": True})
    svc._get_user_settings = AsyncMock(return_value={})
    svc._is_in_quiet_hours = MagicMock(return_value=False)
    svc._resolve_delay_minutes = AsyncMock(return_value=5)
    svc._broadcast_notification = MagicMock()
    svc._broadcast_update = MagicMock()
    svc._email_service = MagicMock()
    svc._email_service.send_notification_email = AsyncMock(
        return_value=(True, "<msg@srw>")
    )
    return svc


def _row(svc) -> dict:
    return svc._persist_notification.call_args.args[0]


class TestRecordLeavesServerTextAlone:
    @pytest.mark.asyncio
    async def test_a_magic_link_and_an_ssh_key_type_survive(self):
        # token_urlsafe can begin with `sk-`; an ssh_key_added notice names
        # `sk-ecdsa-sha2-nistp256@openssh.com`. Neither is worker text.
        body = (
            "Approve: https://cockpit/magic?token=sk-Ab3dE6gH9jK2mN5pQ8rS1tU4vW7xY0z_\n"
            "The SSH key **laptop** (sk-ecdsa-sha2-nistp256@openssh.com) was added."
        )
        svc = _service()
        await svc.record(
            recipient_id=USER,
            category="budget_exceeded",
            dedup_key="k-1",
            subject="Approval needed: shell_execute",
            body=body,
        )
        assert _row(svc)["body"] == body


class TestProducersRedactTheUntrustedPart:
    @pytest.mark.asyncio
    async def test_a_worker_message_is_redacted_in_row_frame_email_and_payload(self):
        svc = _service()
        await svc.record_agent_message(
            user_id=USER,
            job={"description": f"clone {REMOTE}", "config_name": "developer"},
            job_id="job-1",
            thread_id="t-1",
            sequence=1,
            subject=f"key {API_KEY}?",
            message_md=f"push failed: fatal: unable to access '{REMOTE}/'",
            blocking=True,
        )
        row = _row(svc)
        framed = svc._broadcast_notification.call_args.args[0]
        mail = svc._email_service.send_notification_email.call_args.kwargs
        for text in (
            row["subject"],
            row["body"],
            json.dumps(row["payload"]),
            framed["body"],
            mail["subject"],
            mail["body_md"],
        ):
            _assert_clean(text)
        assert "gitea.local/org/repo.git" in row["body"]
        assert "redacted from the worker's message" in row["body"]

    @pytest.mark.asyncio
    async def test_an_already_sanitized_escalation_gains_no_second_note(self):
        body = _delimited_user_body(
            "Q", f"my key is {API_KEY}", reason_line="**Escalated.**"
        )
        svc = _service()
        await svc.record_agent_message(
            user_id=USER,
            job={},
            job_id="job-1",
            thread_id="t-1",
            sequence=None,
            subject="[Escalated] Q",
            message_md=body,
        )
        assert _row(svc)["body"] == body

    @pytest.mark.asyncio
    async def test_review_and_automation_reasons(self):
        svc = _service()
        await svc.record_review_returned(
            user_id=USER, job_id="job-1", config_name="dev", reason=f"saw {REMOTE}"
        )
        row = _row(svc)
        _assert_clean(row["body"] + json.dumps(row["payload"]))
        await svc.record_automation_disabled(
            user_id=USER,
            automation_id="a-1",
            automation_name="nightly",
            reason=f"job failed: {API_KEY}",
        )
        row = _row(svc)
        _assert_clean(row["body"] + json.dumps(row["payload"]))

    def test_a_permission_preview_is_redacted_per_value(self):
        from orchestrator.services.headless_notifications import (
            _truncate_args_for_email,
        )

        preview = _truncate_args_for_email(
            {"command": f"git push {REMOTE} main", "timeout": 30}
        )
        _assert_clean(preview)
        assert json.loads(preview)["timeout"] == 30

    @pytest.mark.asyncio
    async def test_a_sudo_command_is_redacted_in_body_and_payload(self, monkeypatch):
        from orchestrator.services import notification_service as ns
        from orchestrator.services.notification_service import RecordResult
        from orchestrator.services.sudo_gate import SudoGateService

        record = AsyncMock(return_value=RecordResult("n-1", True, {}))
        monkeypatch.setattr(ns.notification_service, "record", record)
        gate = SudoGateService.__new__(SudoGateService)
        gate._db = MagicMock()
        gate._db.get_job = AsyncMock(return_value={"id": "j-1", "user_id": "o-1"})
        await gate._record_owner_notification(
            "r-1",
            job_id="j-1",
            thread_id=None,
            event={
                "id": "r-1",
                "command": "git",
                "arguments": ["clone", REMOTE],
                "working_directory": "/workspace",
            },
        )
        kw = record.await_args.kwargs
        _assert_clean(kw["subject"] + kw["body"] + json.dumps(kw["payload"]))
        assert kw["payload"]["id"] == "r-1"

    def test_a_freeze_notice_redacts_the_worker_fields_only(self):
        from orchestrator.services.job_freeze_notifications import (
            format_freeze_notification,
        )

        subject, body = format_freeze_notification(
            freeze_type="job_complete",
            freeze_data={
                "summary": f"pushed to {REMOTE}",
                "confidence": 0.9,
                "deliverables": [f"out/{API_KEY}.md"],
            },
            job_id="12345678-aaaa",
            config_name="developer",
            description="Ship the fix",
        )
        _assert_clean(subject + body)
        assert "**Confidence:** 90%" in body and "Ship" not in subject

    @pytest.mark.asyncio
    async def test_a_failed_delivery_is_redacted_in_body_and_payload(self):
        # A failed job-ending push reports its error, which echoes the
        # token-bearing remote; the payload's delivery_hold is stored in the
        # feed row and shipped in the SSE frame, so it must be clean too.
        from orchestrator.services.job_freeze_notifications import (
            JobFreezeNotificationDependencies,
            notify_operator_freeze,
        )
        from orchestrator.services.notification_service import RecordResult

        record = AsyncMock(return_value=RecordResult("n-1", True, {}))
        await notify_operator_freeze(
            {"id": "job-1", "user_id": USER, "config_name": "developer"},
            "job-1",
            "job_complete",
            {
                "summary": "done",
                "confidence": 0.9,
                "delivery_failed": True,
                "delivery_error": f"fatal: unable to access '{REMOTE}/': 403",
            },
            dedup_key="freeze_notification:cmd-9",
            dependencies=JobFreezeNotificationDependencies(
                notifier=SimpleNamespace(record=record)
            ),
        )
        kw = record.await_args.kwargs
        _assert_clean(kw["body"])
        _assert_clean(json.dumps(kw["payload"]))
        assert "unable to access" in kw["payload"]["delivery_hold"]
