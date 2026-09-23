"""Unit tests for ``security.access.redact_config_override``.

Pure function, no DB. Mirrors ``TestRedactDatasource`` in
``tests/test_datasource_access.py``. Guards the credential-leak fix: secrets
must be stripped from ``config_override`` at the API boundary and before
persistence, while every non-secret field is preserved verbatim.
"""

import copy

from orchestrator.security import access
from orchestrator.services.thread_projection import redact_thread_metadata


def _full_config_override() -> dict:
    """A config_override carrying every secret-bearing path we inject."""
    return {
        "llm": {
            "model": "gemini-3.5-flash",
            "provider": "google",
            "base_url": "https://ai.h4ll.app/v1",
            "temperature": 0.0,
            "api_key": "sk-llm-SECRET",
            "strategic": {"model": "opus", "api_key": "sk-strategic-SECRET"},
            "tactical": {"model": "sonnet", "api_key": "sk-tactical-SECRET"},
            "summarization": {"model": "haiku", "api_key": "sk-sum-SECRET"},
        },
        "auxiliary": {
            "model": "gemma-4-moe",
            "provider": "openai",
            "base_url": "https://ai.h4ll.app/v1",
            "api_key": "sk-aux-SECRET",
        },
        "env_keys": {
            "EMBEDDING_MODEL": "qwen3-embedding-8b",
            "EMBEDDING_BASE_URL": "https://ai.h4ll.app/v1",
            "EMBEDDING_PROVIDER": "openai",
            "EMBEDDING_API_KEY": "sk-emb-SECRET",
            "OPENROUTER_API_KEY": "sk-or-SECRET",
            "VISION_API_KEY": "sk-vis-SECRET",
            "CITATION_LLM_API_KEY": "sk-cit-SECRET",
            "CITATION_LLM_MODEL": "gpt-4o",
        },
        "workspace": {
            "backend": "virtual",
            "mounts": [
                {"name": "home", "prefix": "u/", "rclone_spec": "s3:KEY:SECRET@bucket"},
            ],
        },
        "interactive": {"permission_mode": "autonomous"},
    }


class TestRedactConfigOverride:
    def test_strips_top_level_and_phase_llm_keys(self):
        out = access.redact_config_override(_full_config_override())
        assert "api_key" not in out["llm"]
        assert "api_key" not in out["llm"]["strategic"]
        assert "api_key" not in out["llm"]["tactical"]
        assert "api_key" not in out["llm"]["summarization"]
        assert "api_key" not in out["auxiliary"]

    def test_strips_env_key_api_keys_only(self):
        out = access.redact_config_override(_full_config_override())
        env = out["env_keys"]
        # All *_API_KEY removed
        assert "EMBEDDING_API_KEY" not in env
        assert "OPENROUTER_API_KEY" not in env
        assert "VISION_API_KEY" not in env
        assert "CITATION_LLM_API_KEY" not in env
        # Non-secret env knobs preserved
        assert env["EMBEDDING_MODEL"] == "qwen3-embedding-8b"
        assert env["EMBEDDING_BASE_URL"] == "https://ai.h4ll.app/v1"
        assert env["EMBEDDING_PROVIDER"] == "openai"
        assert env["CITATION_LLM_MODEL"] == "gpt-4o"

    def test_strips_rclone_spec_in_list(self):
        out = access.redact_config_override(_full_config_override())
        mount = out["workspace"]["mounts"][0]
        assert "rclone_spec" not in mount
        # Sibling, non-secret fields preserved
        assert mount["name"] == "home"
        assert mount["prefix"] == "u/"

    def test_preserves_non_secret_fields(self):
        out = access.redact_config_override(_full_config_override())
        assert out["llm"]["model"] == "gemini-3.5-flash"
        assert out["llm"]["provider"] == "google"
        assert out["llm"]["base_url"] == "https://ai.h4ll.app/v1"
        assert out["llm"]["temperature"] == 0.0
        assert out["llm"]["strategic"]["model"] == "opus"
        assert out["auxiliary"]["model"] == "gemma-4-moe"
        assert out["workspace"]["backend"] == "virtual"
        assert out["interactive"]["permission_mode"] == "autonomous"

    def test_no_secret_string_survives_anywhere(self):
        import json

        out = access.redact_config_override(_full_config_override())
        assert "SECRET" not in json.dumps(out)

    def test_case_insensitive(self):
        co = {"llm": {"API_KEY": "x", "Api_Key": "y"}, "Password": "z", "TOKEN": "t"}
        out = access.redact_config_override(co)
        assert out["llm"] == {}
        assert "Password" not in out
        assert "TOKEN" not in out

    def test_does_not_mutate_input(self):
        co = _full_config_override()
        before = copy.deepcopy(co)
        access.redact_config_override(co)
        assert co == before

    def test_idempotent(self):
        co = _full_config_override()
        once = access.redact_config_override(co)
        twice = access.redact_config_override(once)
        assert once == twice

    def test_handles_non_dict_inputs(self):
        assert access.redact_config_override(None) is None
        assert access.redact_config_override({}) == {}
        assert access.redact_config_override("scalar") == "scalar"
        assert access.redact_config_override([{"api_key": "x", "model": "m"}]) == [
            {"model": "m"}
        ]


class TestRedactThreadMetadataShape:
    """``thread_projection.redact_thread_metadata`` must return metadata as a
    parsed OBJECT — asyncpg hands JSONB back as a JSON string, and the old
    "preserve the original representation" behavior returned that string
    through the owner-facing thread endpoints, silently breaking every
    Cockpit consumer typed against ``metadata?: Record<string, unknown>``
    (settings-pane prefill, attached-datasource defaults, REST
    model/temperature seeding)."""

    def test_string_metadata_returned_as_parsed_object(self):
        import json

        md = {"config_override": {"llm": {"model": "m", "api_key": "sk-SECRET"}}}
        out = redact_thread_metadata({"id": "t", "metadata": json.dumps(md)})
        assert isinstance(out["metadata"], dict)
        assert out["metadata"]["config_override"]["llm"]["model"] == "m"
        assert "api_key" not in out["metadata"]["config_override"]["llm"]

    def test_dict_metadata_stays_object_and_redacts(self):
        md = {
            "config_override": {"llm": {"api_key": "sk-SECRET"}},
            "datasource_ids": ["a"],
        }
        out = redact_thread_metadata({"id": "t", "metadata": md})
        assert out["metadata"]["datasource_ids"] == ["a"]
        assert "api_key" not in out["metadata"]["config_override"]["llm"]

    def test_absent_or_unparseable_metadata_becomes_empty_object(self):
        assert redact_thread_metadata({"id": "t"})["metadata"] == {}
        assert (
            redact_thread_metadata({"id": "t", "metadata": "{not json"})["metadata"]
            == {}
        )
        assert redact_thread_metadata({"id": "t", "metadata": None})["metadata"] == {}

    def test_workspace_binding_dropped(self):
        out = redact_thread_metadata(
            {
                "id": "t",
                "metadata": {
                    "_workspace_binding": {"x": 1},
                    "_stateless_workspace_process_zero_observation": {
                        "runtime_incarnation": "server-owned"
                    },
                    "keep": True,
                },
            }
        )
        assert "_workspace_binding" not in out["metadata"]
        assert "_stateless_workspace_process_zero_observation" not in out["metadata"]
        assert out["metadata"]["keep"] is True

    def test_runtime_retirement_authority_is_redacted_to_safe_state(self):
        internal = {
            "runtime_generation": "generation-secret",
            "runtime_attach_token": "attach-secret",
            "runtime_attach_abort_receipt": {"runtime_attach_token": "attach-secret"},
            "runtime_authority_exposed": True,
            "runtime_retirement_token": "retirement-secret",
            "runtime_retirement_permanent": False,
            "runtime_retirement_started_at": "2026-08-26T00:00:00Z",
            "runtime_retirement_authorized_at": "2026-08-26T00:00:01Z",
            "runtime_retirement_context": {
                "settle_status": "suspended",
                "agent": {"pod_ip": "10.0.0.8"},
            },
            "runtime_retirement_stage_receipt": {"tar_key": "private"},
            "runtime_retirement_local_quiescence": {
                "runtime_attach_token": "attach-secret"
            },
            "runtime_retirement_external_cleanup": {
                "captured_resources": {"grant_handle": "private"}
            },
        }
        out = redact_thread_metadata({"id": "t", "metadata": {}, **internal})
        for key in internal:
            assert key not in out
        assert out["runtime_retirement_pending"] is True
        assert out["retirement_disposition"] == "suspended"
        assert "secret" not in repr(out)
        assert "10.0.0.8" not in repr(out)

    def test_hidden_retirement_preflight_is_not_public_ending(self):
        out = redact_thread_metadata(
            {
                "id": "t",
                "metadata": {},
                "runtime_retirement_token": "hidden-token",
                "runtime_retirement_authorized_at": None,
                "runtime_retirement_context": {"settle_status": "ended"},
            }
        )
        assert out["runtime_retirement_pending"] is False
        assert out["retirement_disposition"] is None
        assert "hidden-token" not in repr(out)


class TestRedactJobWorkspaceAuthority:
    def test_public_job_projection_is_coordinate_and_credential_free(self):
        import orchestrator.main as main

        out = main._redact_job_config_override(
            {
                "id": "job-1",
                "config_override": {
                    "workspace": {
                        "backend": "vm",
                        "remote": {
                            "host": "private-vm.internal",
                            "key_path": "/run/private/key",
                        },
                    }
                },
                "context": {
                    "_workspace_contract": {
                        "version": 1,
                        "requested_backend": "vm",
                        "assigned_backend": "vm",
                        "assignment_source": "request",
                    },
                    "vm": {
                        "status": "ready",
                        "ssh_host": "private-vm.internal",
                        "provision_generation": (
                            "11111111-1111-4111-8111-111111111111"
                        ),
                    },
                    "workspace_container": {
                        "status": "ready",
                        "host": "private-sandbox.internal",
                        "_runtime_incarnation": (
                            "22222222-2222-4222-8222-222222222222"
                        ),
                        "_legacy_k8s_runtime_adoption": {
                            "version": 1,
                            "runtime_incarnation": (
                                "22222222-2222-4222-8222-222222222222"
                            ),
                            "workspace_generation": (
                                "33333333-3333-4333-8333-333333333333"
                            ),
                            "ssh_host_key_fingerprint": "SHA256:" + ("a" * 43),
                        },
                    },
                    "ordinary": "kept",
                },
            }
        )

        assert out["context"] == {"ordinary": "kept"}
        assert out["config_override"]["workspace"] == {"backend": "vm"}
        assert out["workspace_contract"] == {
            "requested_backend": "vm",
            "assigned_backend": "vm",
            "effective_backend": "vm",
            "assignment_source": "request",
            "state": "ready",
            "failure": None,
            "stale_backend": "sandbox",
            "compatibility_derived": False,
        }
        rendered = repr(out)
        assert "private-vm" not in rendered
        assert "private-sandbox" not in rendered
        assert "/run/private" not in rendered
        assert "SHA256:" not in rendered
