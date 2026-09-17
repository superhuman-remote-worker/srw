"""Public job and shared thread projection compatibility at the read boundary."""

from copy import deepcopy
import json
from types import SimpleNamespace
from unittest.mock import Mock, call
from uuid import UUID

import pytest

from orchestrator.security.access import redact_config_override
from orchestrator.services import job_projection as projection
from orchestrator.services.cloud.handles import SessionFolderHandle


def redact(job):
    return projection.redact_job_config_override(
        job,
        vm_mode=lambda: "external",
        runtime_incarnation_key="_runtime_incarnation",
        redact_config_override=redact_config_override,
    )


def test_workspace_recovery_projection_is_coordinate_free_and_redacts_diagnostics():
    operation_id = "11111111-2222-4333-8444-555555555555"
    source = {
        "id": "job",
        "_workspace_recovery": {
            "operation_id": operation_id,
            "state": "paused_attention",
            "reason_code": "prior_runtime_unfenced",
            "started_at": "2026-09-16T08:00:00+00:00",
            "deadline_at": "2026-09-16T08:15:00+00:00",
            "next_check_at": None,
            "cleanup_pending": True,
            "canonical_owner": True,
            "private_endpoint": "10.0.0.9",
            "latest_diagnostic": {"controller_error": "synthetic-private"},
        },
    }

    result = redact(source)

    assert result["workspace_recovery"] == {
        "operation_id": operation_id,
        "state": "paused_attention",
        "reason_code": "prior_runtime_unfenced",
        "message": "Previous workspace execution could not be proven stopped.",
        "started_at": "2026-09-16T08:00:00+00:00",
        "deadline_at": "2026-09-16T08:15:00+00:00",
        "next_check_at": None,
        "retryable": True,
        "cleanup_pending": True,
    }
    assert "_workspace_recovery" not in result
    assert "10.0.0.9" not in json.dumps(result)
    assert "synthetic-private" not in json.dumps(result)


def test_workspace_recovery_projection_rejects_unknown_reason_and_shape():
    assert (
        projection.workspace_recovery_projection(
            {"_workspace_recovery": {"reason_code": "raw_controller_failure"}}
        )
        is None
    )
    assert (
        projection.workspace_recovery_projection({"_workspace_recovery": "not-json"})
        is None
    )


@pytest.mark.parametrize(
    ("canonical_owner", "retryable"),
    [(True, True), (False, False), (None, False)],
)
def test_paused_recovery_is_retryable_only_for_canonical_owner(
    canonical_owner, retryable
):
    raw = {
        "operation_id": "11111111-2222-4333-8444-555555555555",
        "state": "paused_attention",
        "reason_code": "prior_runtime_unfenced",
        "started_at": "2026-09-16T08:00:00+00:00",
        "deadline_at": "2026-09-16T08:15:00+00:00",
        "canonical_owner": canonical_owner,
    }

    result = projection.workspace_recovery_projection({"_workspace_recovery": raw})

    assert result is not None
    assert result["retryable"] is retryable
    assert "canonical_owner" not in result


@pytest.mark.parametrize("as_text", [False, True])
def test_job_projection_preserves_shape_extensions_and_input(as_text):
    context = {
        "vm": {"ssh_host": "synthetic-private"},
        "workspace_container": {"pod_name": "synthetic-private"},
        "_workspace_contract": {"private": "synthetic-private"},
        "_workspace_dispatch_authority": {"private": "synthetic-private"},
        "workspace_runtime": {"private": "synthetic-private"},
        "workspace_backend": "vm",
        "ordinary": {"nested": [1, None]},
    }
    config = {
        "llm": {"model": "test-model", "api_key": "synthetic-private"},
        "workspace": {
            "backend": "remote",
            "remote": {"host": "synthetic-private"},
            "mounts": [{"name": "safe", "rclone_spec": "synthetic-private"}],
        },
        "extension": {"nested": True},
    }
    source = {
        "id": "job",
        "context": json.dumps(context) if as_text else context,
        "config_override": json.dumps(config) if as_text else config,
        "workspace_contract": {"state": "ready", "extension": "retained"},
        "response_extension": {"id": UUID(int=7), "nullable": None},
    }
    before = deepcopy(source)

    result = redact(source)

    assert source == before
    assert result is not source
    assert "synthetic-private" not in repr(result)
    assert result["response_extension"] == before["response_extension"]
    assert result["workspace_contract"] == before["workspace_contract"]
    assert isinstance(result["context"], str if as_text else dict)
    assert isinstance(result["config_override"], str if as_text else dict)
    assert (json.loads(result["context"]) if as_text else result["context"]) == {
        "ordinary": {"nested": [1, None]}
    }
    public_config = (
        json.loads(result["config_override"]) if as_text else result["config_override"]
    )
    assert public_config == {
        "llm": {"model": "test-model"},
        "workspace": {"backend": "remote", "mounts": [{"name": "safe"}]},
        "extension": {"nested": True},
    }


@pytest.mark.parametrize("existing", [None, "opaque", {}, {"extension": "old"}])
def test_missing_workspace_state_is_projected_before_private_context_is_removed(
    monkeypatch, existing
):
    context = {"vm": {"status": "ready"}, "ordinary": 3}
    observed = []

    def project_workspace(job, *, vm_mode):
        observed.append((deepcopy(job), vm_mode))
        return {"state": "ready", "assigned_backend": "vm"}

    project = Mock(side_effect=project_workspace)
    monkeypatch.setattr(projection, "workspace_contract_projection", project)
    source = {"workspace_contract": existing, "context": context}

    result = projection.redact_job_config_override(
        source,
        vm_mode=lambda: "same-cluster",
        runtime_incarnation_key="_runtime_incarnation",
        redact_config_override=redact_config_override,
    )

    project.assert_called_once()
    assert observed == [(source, "same-cluster")]
    assert result["context"] == {"ordinary": 3}
    assert result["workspace_contract"] == {"state": "ready", "assigned_backend": "vm"}


def test_existing_workspace_state_including_null_is_not_recomputed(monkeypatch):
    project = Mock(side_effect=AssertionError("must retain stored public projection"))
    monkeypatch.setattr(projection, "workspace_contract_projection", project)
    vm_mode = Mock(side_effect=AssertionError("must not inspect the provisioner"))
    result = projection.redact_job_config_override(
        {"workspace_contract": {"state": None, "unknown": "retained"}},
        vm_mode=vm_mode,
        runtime_incarnation_key="_runtime_incarnation",
        redact_config_override=redact_config_override,
    )
    assert result["workspace_contract"] == {"state": None, "unknown": "retained"}
    assert "config_override" not in result
    project.assert_not_called()
    vm_mode.assert_not_called()


@pytest.mark.parametrize("config", ["opaque secret", "{invalid", ""])
def test_unparseable_config_is_dropped(config):
    assert redact({"config_override": config})["config_override"] is None


@pytest.mark.parametrize(
    "config, expected",
    [
        (None, None),
        ('"scalar"', '"scalar"'),
        ("null", "null"),
        ("[]", "[]"),
        ('[{"api_key":"synthetic-private","extension":3}]', '[{"extension": 3}]'),
        ([{"api_key": "synthetic-private", "extension": 3}], [{"extension": 3}]),
        (17, 17),
    ],
)
def test_nonobject_config_retains_original_redactor_semantics(config, expected):
    assert redact({"config_override": config})["config_override"] == expected


@pytest.mark.parametrize("context", ["opaque", "{invalid", "[]", "null", [], None])
def test_nonobject_context_is_preserved(context):
    assert redact({"context": context})["context"] == context


@pytest.mark.parametrize("field", ["context", "metadata"])
@pytest.mark.parametrize("as_text", [False, True])
def test_nested_workspace_redaction_keeps_thread_fields_and_jsonb_shape(field, as_text):
    private = {
        "_canvas_workspace_generation": "synthetic-private",
        "_runtime_incarnation": "synthetic-private",
        "_docker_workspace_lease_id": "synthetic-private",
        "_docker_workspace_trust_mode": "synthetic-private",
        "_docker_workspace_attested": "synthetic-private",
        "_docker_workspace_host_key_fingerprint": "synthetic-private",
        "quarantine_reason": "synthetic-private",
    }
    value = {
        "vm": {"status": "ready", "extension": 4, **private},
        "workspace_container": {"status": "ready", **private},
        "other": {"_runtime_incarnation": "unrelated"},
    }
    source = {"id": "thread", field: json.dumps(value) if as_text else value}
    before = deepcopy(source)

    result = projection.redact_nested_workspace_state(
        source, field=field, runtime_incarnation_key="_runtime_incarnation"
    )

    assert source == before
    assert result is not source
    assert isinstance(result[field], str if as_text else dict)
    public = json.loads(result[field]) if as_text else result[field]
    assert public == {
        "vm": {"status": "ready", "extension": 4},
        "workspace_container": {"status": "ready"},
        "other": {"_runtime_incarnation": "unrelated"},
    }


@pytest.mark.parametrize(
    "value", [None, [], "opaque", "{invalid", "null", '{"safe": 1}', {"vm": []}]
)
def test_unchanged_nested_projection_keeps_original_record_identity(value):
    source = {"metadata": value}
    assert (
        projection.redact_nested_workspace_state(
            source, field="metadata", runtime_incarnation_key="_runtime_incarnation"
        )
        is source
    )


def test_runtime_identity_key_is_supplied_by_the_application():
    source = {"metadata": {"vm": {"app_runtime_key": "private", "status": "ready"}}}
    assert projection.redact_nested_workspace_state(
        source, field="metadata", runtime_incarnation_key="app_runtime_key"
    ) == {"metadata": {"vm": {"status": "ready"}}}


def backend(name="active", *, initialized=True):
    return SimpleNamespace(
        backend_id=name,
        is_initialized=initialized,
        get_session_folder_browser_url=Mock(return_value=f"https://{name}.test/folder"),
    )


@pytest.mark.parametrize("handle", [None, ""])
def test_absent_export_handle_does_not_resolve_any_backend(handle):
    resolve = Mock(side_effect=AssertionError("no backend needed"))
    assert (
        projection.resolve_exported_folder_url(handle, resolve_backend=resolve) is None
    )
    resolve.assert_not_called()


def test_legacy_export_handle_uses_active_backend():
    active = backend()
    resolve = Mock(return_value=active)
    assert (
        projection.resolve_exported_folder_url("opaque/path", resolve_backend=resolve)
        == "https://active.test/folder"
    )
    resolve.assert_called_once_with(None)
    active.get_session_folder_browser_url.assert_called_once_with(
        SessionFolderHandle(backend="active", native_id="opaque/path")
    )


@pytest.mark.parametrize("initialized", [False, True])
def test_serialized_handle_routes_to_owning_backend(initialized):
    active = backend()
    owning = backend("other", initialized=initialized)
    resolve = Mock(side_effect=[active, owning])
    handle = SessionFolderHandle("other", "opaque", {"extension": 8})

    result = projection.resolve_exported_folder_url(
        handle.to_db(), resolve_backend=resolve
    )

    assert result == ("https://other.test/folder" if initialized else None)
    assert resolve.call_args_list == [call(None), call("other")]
    active.get_session_folder_browser_url.assert_not_called()
    if initialized:
        owning.get_session_folder_browser_url.assert_called_once_with(handle)
    else:
        owning.get_session_folder_browser_url.assert_not_called()


def test_uninitialized_active_backend_does_not_try_handle_backend():
    active = backend(initialized=False)
    resolve = Mock(return_value=active)
    handle = SessionFolderHandle("other", "opaque", {"extension": 8})
    assert (
        projection.resolve_exported_folder_url(handle.to_db(), resolve_backend=resolve)
        is None
    )
    resolve.assert_called_once_with(None)


def test_initial_backend_lookup_failure_is_not_swallowed():
    error = RuntimeError("no active backend")
    with pytest.raises(RuntimeError) as caught:
        projection.resolve_exported_folder_url(
            "opaque", resolve_backend=Mock(side_effect=error)
        )
    assert caught.value is error


def test_secondary_backend_lookup_failure_degrades_to_no_url():
    handle = SessionFolderHandle("other", "opaque", {"extension": 8})
    resolve = Mock(side_effect=[backend(), RuntimeError("owning backend unavailable")])
    assert (
        projection.resolve_exported_folder_url(handle.to_db(), resolve_backend=resolve)
        is None
    )


def test_browser_url_failure_degrades_to_no_url():
    active = backend()
    active.get_session_folder_browser_url.side_effect = ValueError("unusable handle")
    assert (
        projection.resolve_exported_folder_url(
            "opaque", resolve_backend=Mock(return_value=active)
        )
        is None
    )


@pytest.mark.parametrize(
    "project_folder, expected",
    [(None, "open_folder"), (False, "open_folder"), (True, "diff")],
)
def test_cloud_projection_removes_join_field_and_keeps_extensions(
    project_folder, expected
):
    source = {
        "id": "job",
        "project_has_cloud_folder": project_folder,
        "exported_folder_handle": "opaque",
        "extension": {"nullable": None},
    }
    before = deepcopy(source)
    resolve = Mock(return_value="https://cloud.test/folder")
    result = projection.with_cloud_review_mode(source, resolve_folder_url=resolve)
    assert source == before
    assert result == {
        "id": "job",
        "exported_folder_handle": "opaque",
        "extension": {"nullable": None},
        "cloud_review_mode": expected,
        "exported_folder_url": "https://cloud.test/folder",
    }
    resolve.assert_called_once_with("opaque")
