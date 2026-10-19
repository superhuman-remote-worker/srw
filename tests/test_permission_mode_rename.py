"""Every seam that can still meet the legacy ``supervised`` spelling reads it as
``read_only`` (knowledge-base/knowledge/features/tool_permission_tiers.md, D2).

Stored rows, journals, exported manifests, bundled-YAML copies and agent images
from before the rename keep the old name; none of them may crash or widen.
"""

import uuid
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from orchestrator.schemas.agent_child_threads import AgentThreadCreateRequest
from orchestrator.schemas.thread_admission import ThreadCreateRequest
from orchestrator.schemas.thread_session import ThreadControlRequest
from orchestrator.services.user_administration import validate_grant_value
from shared.runtime.core.capability_grants import evaluate, resolve_grants
from shared.runtime.core.loader import (
    load_and_merge_config,
    normalize_interactive_permission_mode,
)
from shared.thread_controls.queries import applied_control_scalar


class TestRequestSchemas:
    def test_thread_create_reads_the_legacy_spelling(self):
        assert ThreadCreateRequest(permission_mode="supervised").permission_mode == (
            "read_only"
        )

    def test_child_thread_reads_the_legacy_spelling(self):
        request = AgentThreadCreateRequest(permission_mode="supervised")

        assert request.permission_mode == "read_only"
        assert AgentThreadCreateRequest().permission_mode == "read_only"

    def test_openapi_lists_only_canonical_names(self):
        schema = ThreadCreateRequest.model_json_schema()

        assert "supervised" not in str(schema["properties"]["permission_mode"])
        assert "read_only" in str(schema["properties"]["permission_mode"])

    @pytest.mark.parametrize("mode", ["supervised", "read_only"])
    def test_mode_set_reaches_the_inbox_in_the_spelling_old_images_accept(self, mode):
        request = ThreadControlRequest(
            client_request_id=uuid.uuid4(), method="mode.set", mode=mode
        )

        assert request.mode == "read_only"
        assert request.control_payload() == {"mode": "supervised"}

    def test_other_modes_reach_the_inbox_unchanged(self):
        request = ThreadControlRequest(
            client_request_id=uuid.uuid4(), method="mode.set", mode="auto_accept"
        )

        assert request.control_payload() == {"mode": "auto_accept"}


class TestControlReceipt:
    def _scalar(self, requested, applied):
        ids = {"request_id": "r1", "client_request_id": "c1", "request_seq": 7}
        return applied_control_scalar(
            request_id="r1",
            client_request_id="c1",
            request_seq=7,
            verb="mode.set",
            request_payload={"mode": requested},
            event_kind="mode.changed",
            event_payload={**ids, "method": "mode.set", "mode": applied},
        )

    @pytest.mark.parametrize(
        ("requested", "applied"),
        [
            ("supervised", "supervised"),  # an old image applied a legacy request
            ("supervised", "read_only"),  # a new image applied a legacy request
            ("read_only", "read_only"),
        ],
    )
    def test_either_spelling_finalizes_to_the_canonical_column_value(
        self, requested, applied
    ):
        assert self._scalar(requested, applied) == ("permission_mode", "read_only")

    def test_a_different_mode_is_still_a_mismatch(self):
        assert self._scalar("supervised", "auto_accept") is None


class TestGrants:
    def _row(self, value):
        return [{"key": "permission_mode", "value_json": value}]

    def test_a_stored_legacy_ceiling_resolves_without_crashing(self):
        grants = resolve_grants(
            user_rows=self._row("autonomous"),
            project_rows=[],
            global_rows=self._row("supervised"),
        )

        assert grants["permission_mode"] == "read_only"

    def test_a_legacy_config_value_is_compared_as_read_only(self):
        fragment = {"interactive": {"permission_mode": "supervised"}}

        assert evaluate(fragment, {"permission_mode": "read_only"}) == []
        assert evaluate(fragment, {"permission_mode": "supervised"}) == []

    def test_the_ceiling_still_binds_under_a_legacy_spelling(self):
        fragment = {"interactive": {"permission_mode": "auto_accept"}}

        violations = evaluate(fragment, {"permission_mode": "supervised"})

        assert violations and "exceeds the ceiling" in violations[0]

    def test_grant_writes_accept_both_spellings_and_reject_others(self):
        validate_grant_value("permission_mode", "supervised")
        validate_grant_value("permission_mode", "read_only")
        with pytest.raises(HTTPException):
            validate_grant_value("permission_mode", "bogus")


class TestConfigLayers:
    def test_a_legacy_layer_is_normalised_without_mutating_it(self):
        layer = {"interactive": {"permission_mode": "supervised", "x": 1}}

        out = normalize_interactive_permission_mode(layer, source="test")

        assert out == {"interactive": {"permission_mode": "read_only", "x": 1}}
        assert layer["interactive"]["permission_mode"] == "supervised"

    def test_a_canonical_layer_is_returned_by_identity(self):
        layer = {"interactive": {"permission_mode": "auto_accept"}}

        assert normalize_interactive_permission_mode(layer, source="test") is layer

    def test_a_bundled_yaml_with_the_legacy_value_merges_to_read_only(self, tmp_path):
        leaf = tmp_path / "legacy.yaml"
        leaf.write_text(
            "$extends: session_base\ninteractive:\n  permission_mode: supervised\n"
        )

        merged = load_and_merge_config(str(leaf))

        assert merged["interactive"]["permission_mode"] == "read_only"


class TestAgentControl:
    @pytest.mark.parametrize("mode", ["supervised", "read_only", "auto_accept"])
    def test_mode_set_is_applied_in_either_spelling(self, mode):
        from agent.api.persistent_app import _describe_control_request

        request = SimpleNamespace(verb="mode.set", payload={"mode": mode})

        assert _describe_control_request(request) == ("mode.changed", "applied", None)

    def test_an_unknown_mode_is_still_rejected(self):
        from agent.api.persistent_app import _describe_control_request

        request = SimpleNamespace(verb="mode.set", payload={"mode": "bogus"})

        assert _describe_control_request(request)[1] == "rejected"
