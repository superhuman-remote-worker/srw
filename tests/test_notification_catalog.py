"""The notification catalog is data; these pin the invariants the rest of the
system relies on (unified notification system, D5/D7/D8)."""

import uuid
from datetime import datetime, timezone

import pytest

from orchestrator.services import notification_catalog as cat


class TestCategories:
    def test_every_category_is_well_formed(self):
        for name, spec in cat.CATEGORIES.items():
            assert spec.name == name
            assert spec.severity in cat.SEVERITIES
            assert spec.actions, f"{name} declares no actions"
            for action in spec.actions:
                assert action.label_key.startswith("notifications.actions.")
                assert action.style in cat.ACTION_STYLES

    def test_unknown_category_is_loud(self):
        with pytest.raises(ValueError, match="unknown notification category"):
            cat.category_spec("nope")

    def test_slice_one_classes_kept_mailing(self):
        # The documented "before": every class that mailed then mailed
        # immediately; only `low` was in-app only.
        for severity in ("critical", "high", "normal"):
            assert all(s.immediate for s in cat.SEVERITY_CLASSES_V1[severity])
            assert "email" in {s.channel for s in cat.SEVERITY_CLASSES_V1[severity]}
        assert cat.SEVERITY_CLASSES_V1["low"] == ()

    def test_slice_two_classes_escalate_normal_and_keep_urgent_immediate(self):
        # D8: immediate delivery is reserved for classes where latency costs
        # something real; `normal` waits the officer's window and mails only
        # if nobody looked and nobody settled it (D5/D6). `low` stays in-app.
        assert cat.SEVERITY_CLASSES is cat.SEVERITY_CLASSES_V2
        for severity in ("critical", "high"):
            steps = cat.steps_for(cat.category_spec("incident"), severity)
            assert steps and all(s.immediate for s in steps)
            assert "email" in {s.channel for s in steps}
        normal = cat.steps_for(cat.category_spec("review_queue"), "normal")
        assert normal and not any(s.immediate for s in normal)
        assert "email" in {s.channel for s in normal}
        for step in normal:
            assert step.delay == cat.DELAY_OFFICER_RESPONSE
            assert set(step.conditions) == {"not_seen", "not_resolved"}
            assert step.batch_key == "{category}"
            assert step.batch_window_minutes == 15
        assert cat.SEVERITY_CLASSES["low"] == ()

    def test_review_queue_is_normal_so_it_waits(self):
        # The issue doc's case: a job completing under autonomy `review` must
        # not mail on the event.
        spec = cat.category_spec("review_queue")
        assert spec.severity == "normal"
        assert not any(s.immediate for s in cat.steps_for(spec, spec.severity))

    def test_only_critical_bypasses_quiet_hours_by_default(self):
        spec = cat.category_spec("review_queue")
        assert cat.bypasses_quiet_hours(spec, "critical") is True
        assert cat.bypasses_quiet_hours(spec, "high") is False

    def test_severity_override_must_be_valid(self):
        spec = cat.category_spec("review_queue")
        assert cat.normalize_severity(spec, None) == "normal"
        assert cat.normalize_severity(spec, "low") == "low"
        with pytest.raises(ValueError):
            cat.normalize_severity(spec, "urgent")


class TestSpecs:
    def test_action_input_and_name_go_together(self):
        with pytest.raises(ValueError):
            cat.ActionSpec("x", "notifications.actions.x", input="text")
        with pytest.raises(ValueError):
            cat.ActionSpec("x", "notifications.actions.x", input_name="reason")

    def test_action_label_key_namespace_enforced(self):
        with pytest.raises(ValueError):
            cat.ActionSpec("x", "inbox.sudoDetail.approve")

    def test_step_rejects_in_app_and_unknown_channels(self):
        with pytest.raises(ValueError):
            cat.StepSpec("in_app")
        with pytest.raises(ValueError):
            cat.StepSpec("carrier_pigeon")

    def test_step_validates_delay_conditions_and_batching(self):
        with pytest.raises(ValueError, match="bad delay"):
            cat.StepSpec("email", delay="soon")
        with pytest.raises(ValueError, match="bad delay"):
            cat.StepSpec("email", delay=-1)
        with pytest.raises(ValueError, match="unknown step condition"):
            cat.StepSpec("email", delay=5, conditions=("not_bored",))
        with pytest.raises(ValueError, match="go together"):
            cat.StepSpec("email", delay=5, batch_key="x")
        with pytest.raises(ValueError, match="positive"):
            cat.StepSpec("email", delay=5, batch_key="x", batch_window_minutes=0)
        # An immediate step has no due time at which a condition could be
        # re-evaluated or a batch collected.
        with pytest.raises(ValueError, match="non-zero delay"):
            cat.StepSpec("email", conditions=("not_seen",))
        ok = cat.StepSpec(
            "email",
            delay=cat.DELAY_OFFICER_RESPONSE,
            conditions=("not_seen", "severity_at_least:high"),
            batch_key="{category}",
            batch_window_minutes=15,
        )
        assert not ok.immediate


class TestConditions:
    def _row(self, **overrides):
        row = {
            "severity": "normal",
            "seen_at": None,
            "read_at": None,
            "resolved_at": None,
        }
        row.update(overrides)
        return row

    def test_condition_matrix(self):
        assert cat.evaluate_condition("not_seen", self._row()) is True
        assert cat.evaluate_condition("not_seen", self._row(seen_at="t")) is False
        assert cat.evaluate_condition("not_read", self._row(seen_at="t")) is True
        assert cat.evaluate_condition("not_read", self._row(read_at="t")) is False
        assert cat.evaluate_condition("not_resolved", self._row()) is True
        assert (
            cat.evaluate_condition("not_resolved", self._row(resolved_at="t")) is False
        )
        # The live source wins even when the row was never stamped.
        assert (
            cat.evaluate_condition("not_resolved", self._row(), source_resolved=True)
            is False
        )
        assert cat.evaluate_condition("severity_at_least:normal", self._row()) is True
        assert cat.evaluate_condition("severity_at_least:high", self._row()) is False
        assert (
            cat.evaluate_condition(
                "severity_at_least:high", self._row(severity="critical")
            )
            is True
        )

    def test_first_failing_condition_names_the_culprit_and_fails_closed(self):
        row = self._row(seen_at="t")
        assert cat.first_failing_condition(["not_resolved", "not_seen"], row) == (
            "not_seen"
        )
        assert cat.first_failing_condition([], row) is None
        assert cat.first_failing_condition(["not_resolved"], row) is None
        # An unknown condition skips the step rather than mailing on a rule
        # nobody can read.
        assert cat.first_failing_condition(["not_bored"], row) == "invalid:not_bored"

    def test_unknown_condition_is_loud(self):
        with pytest.raises(ValueError):
            cat.validate_condition("severity_at_least:urgent")
        with pytest.raises(ValueError):
            cat.evaluate_condition("nope", self._row())


class TestBatchingAndQuietHours:
    def test_bucket_rounds_up_to_the_window_and_never_earlier(self):
        from datetime import datetime, timezone

        due = datetime(2026, 8, 26, 9, 3, 20, tzinfo=timezone.utc)
        assert cat.bucket_due_at(due, 15) == datetime(
            2026, 8, 26, 9, 15, tzinfo=timezone.utc
        )
        on_boundary = datetime(2026, 8, 26, 9, 15, tzinfo=timezone.utc)
        assert cat.bucket_due_at(on_boundary, 15) == on_boundary
        # Two rows whose delays end inside one window share the due instant.
        a = cat.bucket_due_at(datetime(2026, 8, 26, 9, 1, tzinfo=timezone.utc), 15)
        b = cat.bucket_due_at(datetime(2026, 8, 26, 9, 14, tzinfo=timezone.utc), 15)
        assert a == b

    def test_batch_key_template_sees_the_row(self):
        step = cat.StepSpec(
            "email", delay=5, batch_key="{category}:{severity}", batch_window_minutes=5
        )
        assert cat.batch_key_for(
            step, {"category": "review_queue", "severity": "normal"}
        ) == ("review_queue:normal")
        assert cat.batch_key_for(cat.StepSpec("email", delay=5), {}) is None

    def _qh(self, start, end, tz="Europe/Berlin", enabled=True):
        return {
            "communication": {
                "quiet_hours": {
                    "enabled": enabled,
                    "start": start,
                    "end": end,
                    "timezone": tz,
                }
            }
        }

    def test_overnight_window_ends_tomorrow_before_midnight_and_today_after(self):
        from datetime import datetime, timezone

        settings = self._qh("22:00", "08:00")
        # 23:30 Berlin (CEST = UTC+2) on the 26th → ends 08:00 Berlin on the 27th.
        inside, end = cat.quiet_hours_window(
            settings, datetime(2026, 8, 26, 21, 30, tzinfo=timezone.utc)
        )
        assert inside is True
        assert end == datetime(2026, 8, 27, 6, 0, tzinfo=timezone.utc)
        # 03:00 Berlin on the 27th → ends 08:00 Berlin the same day.
        inside, end = cat.quiet_hours_window(
            settings, datetime(2026, 8, 27, 1, 0, tzinfo=timezone.utc)
        )
        assert inside is True
        assert end == datetime(2026, 8, 27, 6, 0, tzinfo=timezone.utc)
        # Midday: not quiet.
        assert cat.quiet_hours_window(
            settings, datetime(2026, 8, 27, 10, 0, tzinfo=timezone.utc)
        ) == (False, None)

    def test_same_day_window_and_dst_boundary(self):
        from datetime import datetime, timezone

        settings = self._qh("12:00", "14:00")
        inside, end = cat.quiet_hours_window(
            settings,
            datetime(2026, 8, 26, 11, 0, tzinfo=timezone.utc),  # 13:00 Berlin
        )
        assert inside is True and end == datetime(
            2026, 8, 26, 12, 0, tzinfo=timezone.utc
        )
        # The night the clocks go back (2026-10-25, 03:00 CEST → 02:00 CET):
        # 22:00–08:00 entered at 23:00 CEST ends at 08:00 CET = 07:00 UTC.
        inside, end = cat.quiet_hours_window(
            self._qh("22:00", "08:00"),
            datetime(2026, 10, 24, 21, 0, tzinfo=timezone.utc),
        )
        assert inside is True
        assert end == datetime(2026, 10, 25, 7, 0, tzinfo=timezone.utc)

    def test_unusable_configuration_is_not_quiet(self):
        assert cat.quiet_hours_window(self._qh("22:00", "08:00", enabled=False)) == (
            False,
            None,
        )
        assert cat.quiet_hours_window(self._qh("", "08:00")) == (False, None)
        assert cat.quiet_hours_window(
            self._qh("22:00", "08:00", tz="Mars/Olympus")
        ) == (
            False,
            None,
        )
        assert cat.quiet_hours_window({}) == (False, None)


class TestPreferenceMatrix:
    def test_category_cell_overrides_channel_default(self):
        channels = {"email": True, "ntfy": False}
        categories = {"review_queue": {"email": False, "ntfy": True}}
        assert (
            cat.channel_enabled(channels, categories, "review_queue", "email") is False
        )
        assert cat.channel_enabled(channels, categories, "review_queue", "ntfy") is True
        assert cat.channel_enabled(channels, categories, "incident", "email") is True
        assert cat.channel_enabled(channels, categories, "incident", "ntfy") is False

    def test_defaults_to_on_and_ignores_non_booleans(self):
        assert cat.channel_enabled(None, None, "incident", "email") is True
        assert cat.channel_enabled({"email": "false"}, {}, "incident", "email") is True
        assert (
            cat.channel_enabled({}, {"incident": {"email": "no"}}, "incident", "email")
            is True
        )

    def test_serialize_actions_merges_params_into_every_action(self):
        spec = cat.category_spec("vm_upgrade")
        actions = cat.serialize_actions(spec, {"request_id": "r1", "job_id": "j1"})
        assert [a["type"] for a in actions] == [
            "approve_upgrade",
            "resume_without_vm",
            "deny",
        ]
        assert all(a["params"] == {"request_id": "r1", "job_id": "j1"} for a in actions)
        deny = actions[-1]
        assert deny["input"] == "text" and deny["input_name"] == "reason"


class TestIds:
    def test_notification_id_is_deterministic(self):
        a = cat.notification_id("user", "u1", "freeze_notification:cmd")
        b = cat.notification_id("user", "u1", "freeze_notification:cmd")
        assert a == b
        assert a != cat.notification_id("user", "u2", "freeze_notification:cmd")
        assert a != cat.notification_id("user", "u1", "freeze_notification:other")

    def test_notification_id_matches_postgres_uuid_generate_v5(self):
        # the slice-3 backfill mints ids in SQL with uuid_generate_v5(uuid_ns_url(), ...);
        # the Python side must produce byte-identical uuids from the same string.
        key = f"{cat.ID_PREFIX}:user:u1:k"
        assert cat.notification_id("user", "u1", "k") == uuid.uuid5(
            uuid.NAMESPACE_URL, key
        )


class TestSerialization:
    def test_serialize_notification_shape(self):
        now = datetime(2026, 8, 26, 9, 0, tzinfo=timezone.utc)
        row = {
            "id": uuid.UUID(int=1),
            "category": "review_queue",
            "severity": "normal",
            "subject": "s",
            "body": "b",
            "source_kind": "job",
            "source_id": "j1",
            "actions": [{"type": "approve"}],
            "payload": {"job_id": "j1"},
            "created_at": now,
            "seen_at": None,
            "read_at": None,
            "interacted_at": None,
            "resolved_at": now,
            "resolved_by": "user:u1",
            "archived_at": None,
        }
        wire = cat.serialize_notification(row)
        assert wire["id"] == str(uuid.UUID(int=1))
        assert wire["source_ref"] == {"kind": "job", "id": "j1"}
        assert wire["created_at"] == now.isoformat()
        assert wire["seen_at"] is None
        assert wire["resolved_by"] == "user:u1"
        assert cat.cursor_for(row) == f"{now.isoformat()}|{uuid.UUID(int=1)}"

    def test_source_ref_is_null_without_source(self):
        wire = cat.serialize_notification(
            {
                "id": "x",
                "category": "incident",
                "severity": "critical",
                "created_at": None,
            }
        )
        assert wire["source_ref"] is None
        assert wire["actions"] == [] and wire["payload"] == {}


class TestRegistries:
    def test_register_action_rejects_undeclared_action(self):
        with pytest.raises(ValueError, match="declares no action"):
            cat.register_action("review_queue", "teleport")

    def test_register_and_lookup(self):
        saved = dict(cat._ACTION_HANDLERS)
        try:

            @cat.register_action("incident", "open")
            async def _handler(ctx):
                return cat.ActionResult(result={"ok": True})

            assert cat.action_handler("incident", "open") is _handler
            assert cat.action_handler("incident", "nope") is None
        finally:
            cat._ACTION_HANDLERS.clear()
            cat._ACTION_HANDLERS.update(saved)


def test_chat_executors_unavailable_category():
    spec = cat.category_spec("chat_executors_unavailable")
    assert spec.severity == "high"
    assert [(a.type, a.label_key) for a in spec.actions] == [
        ("open", "notifications.actions.openAdminCapacity")
    ]
