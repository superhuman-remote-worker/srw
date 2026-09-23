"""Confidence rendering in knowledge-note and frozen-job formatters.

Regression coverage for the MCP `list_knowledge_notes` crash: confidence is a
Postgres enum string (`'high'`/`'medium'`/`'low'`), but the formatters applied a
`%` float format spec, raising `Unknown format code '%' for object of type 'str'`.
See knowledge-base/knowledge/issues/mcp_knowledge_notes_confidence_percent_format_crash.md.
"""

import pytest

from shared.orch_surface.formatters import (
    _fmt_confidence,
    format_frozen_job,
    format_job_detail,
    format_job_summary,
    format_jobs,
    format_knowledge_note_detail,
    format_knowledge_notes,
)


class TestFmtConfidence:
    def test_enum_string_passthrough(self):
        assert _fmt_confidence("high") == "high"
        assert _fmt_confidence("medium") == "medium"
        assert _fmt_confidence("low") == "low"

    def test_float_in_unit_range_is_percent(self):
        assert _fmt_confidence(0.82) == "82%"
        assert _fmt_confidence(0.0) == "0%"
        assert _fmt_confidence(1.0) == "100%"

    def test_int_zero_one_is_percent(self):
        assert _fmt_confidence(0) == "0%"
        assert _fmt_confidence(1) == "100%"

    def test_out_of_range_numeric_is_plain(self):
        assert _fmt_confidence(82) == "82"
        assert _fmt_confidence(-1) == "-1"
        assert _fmt_confidence(3.5) == "3.5"

    def test_bool_is_not_percent(self):
        # bool is an int subclass; guard so True doesn't render as "100%".
        assert _fmt_confidence(True) == "True"
        assert _fmt_confidence(False) == "False"


class TestFormatKnowledgeNotes:
    def _note(self, confidence):
        return {
            "notes": [
                {
                    "note_id": "n1",
                    "title": "A note",
                    "note_type": "insight",
                    "status": "active",
                    "confidence": confidence,
                    "content_preview": "preview",
                }
            ],
            "total": 1,
            "offset": 0,
            "limit": 50,
        }

    @pytest.mark.parametrize(
        "confidence,expected",
        [
            ("high", "confidence: high"),
            (0.82, "confidence: 82%"),
            (82, "confidence: 82"),
        ],
    )
    def test_renders_confidence_without_crashing(self, confidence, expected):
        out = format_knowledge_notes(self._note(confidence))
        assert expected in out

    def test_string_confidence_does_not_raise(self):
        # The original bug: f"{'high':.0%}" -> ValueError.
        out = format_knowledge_notes(self._note("high"))
        assert "A note" in out

    def test_none_confidence_omits_line(self):
        out = format_knowledge_notes(self._note(None))
        assert "confidence:" not in out


class TestFormatKnowledgeNoteDetail:
    def _detail(self, confidence):
        return {
            "note_id": "n1",
            "title": "A note",
            "note_type": "insight",
            "status": "active",
            "confidence": confidence,
            "content": "body",
        }

    @pytest.mark.parametrize(
        "confidence,expected",
        [
            ("high", "Confidence: high"),
            (0.82, "Confidence: 82%"),
            (82, "Confidence: 82"),
        ],
    )
    def test_renders_confidence_without_crashing(self, confidence, expected):
        out = format_knowledge_note_detail(self._detail(confidence))
        assert expected in out

    def test_none_confidence_omits_line(self):
        out = format_knowledge_note_detail(self._detail(None))
        assert "Confidence:" not in out


class TestFormatFrozenJob:
    @pytest.mark.parametrize(
        "confidence,expected",
        [
            ("high", "Confidence: high"),
            (0.82, "Confidence: 82%"),
            (82, "Confidence: 82"),
        ],
    )
    def test_renders_confidence_without_crashing(self, confidence, expected):
        out = format_frozen_job("job1", {"summary": "s", "confidence": confidence})
        assert expected in out

    def test_none_confidence_omits_line(self):
        out = format_frozen_job("job1", {"summary": "s"})
        assert "Confidence:" not in out


class TestJobErrorFormatters:
    def test_job_detail_uses_error_message(self):
        out = format_job_detail(
            {
                "id": "job1",
                "status": "failed",
                "config_name": "critic",
                "error_message": "workspace unavailable",
            }
        )
        assert "Error: workspace unavailable" in out

    def test_job_summary_uses_error_message(self):
        # E1: format_job_summary now takes (job_id, envelope) — the truthful
        # read envelope from officer_supervision_surface §4.
        out = format_job_summary(
            "job1",
            {
                "observed_at": "2026-08-14T12:00:00+00:00",
                "sources": [{"name": "control_db", "status": "fresh"}],
                "data": {
                    "job": {
                        "id": "job1",
                        "status": "failed",
                        "config_name": "critic",
                        "error_message": "grant denied",
                    }
                },
            },
        )
        assert "Error: grant denied" in out
        assert "Config: critic" in out

    def test_job_list_uses_error_message(self):
        out = format_jobs(
            [
                {
                    "id": "job1",
                    "status": "failed",
                    "config_name": "critic",
                    "error_message": "clone failed",
                }
            ]
        )
        assert "Error: clone failed" in out

    def test_blocked_delivery_is_not_rendered_as_cancellation_or_success(self):
        job = {
            "id": "job1",
            "status": "cancelled",
            "completion_outcome_kind": "blocked_undelivered",
            "config_name": "executor",
            "error_message": "declared pull request was not delivered",
        }
        detail = format_job_detail(job)
        listed = format_jobs([job])
        summary = format_job_summary(
            "job1",
            {
                "observed_at": "2026-08-24T12:00:00+00:00",
                "sources": [{"name": "control_db", "status": "fresh"}],
                "data": {"job": job},
            },
        )
        for rendered in (detail, listed, summary):
            assert "blocked_undelivered" in rendered
            assert "Status: cancelled" not in rendered

    def test_operator_pause_hold_is_visible_in_job_detail(self):
        held = {
            "id": "job1",
            "status": "paused",
            "context": {
                "_operator_pause_hold": {
                    "version": 1,
                    "hold_id": "h1",
                    "paused_by": "user-a",
                    "paused_at": "2026-09-23T10:00:00+00:00",
                }
            },
        }
        detail = format_job_detail(held)
        assert "Operator pause hold" in detail
        assert "paused_by=user-a" in detail
        assert "Operator pause hold" not in format_job_detail(
            {"id": "job1", "status": "paused", "context": {}}
        )

    def test_workspace_contract_is_prominent_without_transport_details(self):
        job = {
            "id": "job1",
            "status": "created",
            "workspace_contract": {
                "requested_backend": "vm",
                "assigned_backend": "vm",
                "effective_backend": None,
                "state": "mismatch",
                "failure": "sandbox_ready_for_vm_assignment",
                "stale_backend": "sandbox",
            },
        }
        detail = format_job_detail(job)
        listed = format_jobs([job])
        for rendered in (detail, listed):
            assert "requested=vm" in rendered
            assert "assigned=vm" in rendered
            assert "effective=unavailable" in rendered
            assert "stale sandbox" in rendered
            assert "host" not in rendered
