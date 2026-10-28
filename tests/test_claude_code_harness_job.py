import copy

import pytest
from srw_claude_code_harness.job import (
    JobRejected,
    JobSpec,
    Limits,
    ModelRoute,
    parse_job_start,
    require_anthropic_route,
)
from srw_claude_code_harness.orchestrator import PinnedEcho
from srw_claude_code_harness.workspace import WorkspaceAccess

LLM = {
    "model": "muse-spark-1.3",
    "provider": "anthropic",
    "client_protocol": "anthropic-messages",
    "base_url": "https://api.meta.ai/v1",
    "api_key": "k",
    "reasoning_level": "high",
}
REMOTE = {
    "host": "10.0.0.5",
    "port": 2222,
    "username": "agent-host",
    "key_path": "/keys/id",
    "workspace_path": "/home/agent-host/workspace",
}
START_BODY = {
    "job_id": "j1",
    "description": "do the thing",
    "instructions": "be brief",
    "resolved_config": {
        "agent": {"llm": LLM, "workspace": {"remote": REMOTE}},
        "prompts": {},
    },
    "config_override": None,
    "workspace_ssh_host_key_fingerprint": "SHA256:abc",
    "pinned_delivery_id": "d1",
    "pinned_projection_digest": "p1",
    "pinned_delivery_proof": "pr1",
    "recipient": {"user_id": "u"},
}


def test_parse_job_start_reads_the_resolved_blob_first():
    spec = parse_job_start(START_BODY)
    assert isinstance(spec, JobSpec)
    assert spec.model == ModelRoute(
        "muse-spark-1.3",
        "anthropic",
        "anthropic-messages",
        "https://api.meta.ai/v1",
        "k",
    )
    assert spec.workspace == WorkspaceAccess(
        host="10.0.0.5",
        port=2222,
        user="agent-host",
        root="/home/agent-host/workspace",
        private_key_file="/keys/id",
        host_key_fingerprint="SHA256:abc",
    )
    assert spec.job_id == "j1" and spec.prompt == "do the thing"
    assert spec.instructions == "be brief" and spec.effort == "high"
    assert spec.pinned == PinnedEcho("d1", "p1", "pr1")
    assert spec.recipient == {"user_id": "u"}
    assert spec.limits == Limits(200, 20.0, 3600)


def test_parse_job_start_falls_back_to_config_override():
    body = copy.deepcopy(START_BODY)
    body["resolved_config"] = None
    body["config_override"] = {"llm": LLM, "workspace": {"remote": REMOTE}}
    spec = parse_job_start(body)
    assert spec.model.model == "muse-spark-1.3" and spec.workspace.host == "10.0.0.5"


def test_key_path_override_replaces_the_blob_key():
    spec = parse_job_start(START_BODY, key_path_override="/run/key")
    assert spec.workspace.private_key_file == "/run/key"


def test_effort_ignores_unknown_levels_and_no_pin_is_none():
    body = copy.deepcopy(START_BODY)
    body["resolved_config"]["agent"]["llm"]["reasoning_level"] = "ultra"
    for key in (
        "pinned_delivery_id",
        "pinned_projection_digest",
        "pinned_delivery_proof",
    ):
        del body[key]
    spec = parse_job_start(body)
    assert spec.effort is None and spec.pinned is None


def test_parse_job_start_refuses_a_job_without_a_workspace():
    body = copy.deepcopy(START_BODY)
    del body["resolved_config"]["agent"]["workspace"]
    with pytest.raises(JobRejected, match="the job carries no workspace"):
        parse_job_start(body)


def test_parse_job_start_refuses_a_job_without_a_model():
    body = copy.deepcopy(START_BODY)
    del body["resolved_config"]["agent"]["llm"]
    with pytest.raises(JobRejected, match="the job carries no model"):
        parse_job_start(body)


def test_require_anthropic_route_refuses_other_shapes():
    with pytest.raises(JobRejected, match="this harness speaks anthropic-messages"):
        require_anthropic_route(
            ModelRoute("gpt-5.5", "codex", "openai-responses", "http://proxy/v1", "k")
        )


def test_require_anthropic_route_needs_a_credential_or_base_url():
    with pytest.raises(JobRejected, match="has no credential"):
        require_anthropic_route(
            ModelRoute("m", "anthropic", "anthropic-messages", None, None)
        )
    require_anthropic_route(
        ModelRoute("m", "anthropic", "anthropic-messages", "http://p/v1", None)
    )
    require_anthropic_route(
        ModelRoute("m", "anthropic", "anthropic-messages", None, "k")
    )


def test_secrets_stay_out_of_the_repr():
    body = copy.deepcopy(START_BODY)
    body["resolved_config"]["agent"]["llm"]["api_key"] = "sk-secret-key"
    body["pinned_delivery_proof"] = "proof-secret"
    body["recipient"] = {"email": "someone-secret@example.com"}
    spec = parse_job_start(body)
    text = repr(spec) + repr(spec.model) + repr(spec.pinned)
    for secret in ("sk-secret-key", "proof-secret", "someone-secret"):
        assert secret not in text
    assert spec.model.api_key == "sk-secret-key"
