import pytest
from srw_claude_code_harness.config import (
    ConfigError,
    config_hash,
    effective_settings,
    parse_config,
)
from srw_claude_code_harness.task import parse_task
from srw_claude_code_harness.bindings import parse_bindings

MINIMAL = {"version": 1, "model": "claude-sonnet-5-5"}


def test_minimal_config_gets_the_documented_defaults():
    config = parse_config(MINIMAL)
    assert config.tools == ("Bash", "Read", "Write", "Edit")
    assert (
        config.limits.max_turns,
        config.limits.max_budget_usd,
        config.limits.timeout_seconds,
    ) == (200, 20.0, 3600)
    assert (
        config.system_prompt.kind == "preset"
        and config.effort is None
        and config.thinking is None
    )
    assert config.auto_compact is True and config.workspace_env == ()


def test_full_config_round_trips_into_effective_settings():
    data = {
        **MINIMAL,
        "systemPrompt": {"type": "preset", "append": "Load the CSV."},
        "tools": ["Bash", "Read"],
        "limits": {"maxTurns": 50, "maxBudgetUsd": 5, "timeoutSeconds": 600},
        "effort": "high",
        "thinking": {"type": "enabled", "budget_tokens": 8000},
        "autoCompact": False,
        "workspaceEnv": ["NEO4J_URI"],
    }
    settings = effective_settings(parse_config(data))
    assert settings["model"] == "claude-sonnet-5-5" and settings["tools"] == [
        "Bash",
        "Read",
    ]
    assert settings["systemPrompt"] == {"type": "preset", "append": "Load the CSV."}
    assert settings["autoCompact"] is False and settings["permissionMode"] == "default"
    assert settings["settingSources"] == [] and settings["outputLimitChars"] == 30000


@pytest.mark.parametrize(
    "bad,needle",
    [
        ({**MINIMAL, "version": 2}, "version"),
        ({"version": 1}, "model"),
        ({**MINIMAL, "surprise": 1}, "unknown setting: surprise"),
        ({**MINIMAL, "limits": {"maxTurns": 0}}, "maxTurns"),
        ({**MINIMAL, "limits": {"timeoutSeconds": 10}}, "timeoutSeconds"),
        ({**MINIMAL, "limits": {"maxBudgetUsd": 0}}, "maxBudgetUsd"),
        ({**MINIMAL, "tools": []}, "tools"),
        ({**MINIMAL, "tools": ["Bash", "Grep"]}, "tools"),
        ({**MINIMAL, "effort": "ultra"}, "effort"),
        ({**MINIMAL, "thinking": {"type": "enabled"}}, "thinking"),
        ({**MINIMAL, "systemPrompt": {"type": "custom"}}, "systemPrompt"),
        ({**MINIMAL, "workspaceEnv": ["lower"]}, "workspaceEnv"),
        ({**MINIMAL, "workspaceEnv": ["FOO\n"]}, "workspaceEnv"),
        ({**MINIMAL, "workspaceEnv": ["CLAUDE_CODE_OAUTH_TOKEN"]}, "workspaceEnv"),
        (
            {**MINIMAL, "workspaceEnv": ["NEO4J_URI", "ANTHROPIC_API_KEY"]},
            "workspaceEnv",
        ),
        (
            {**MINIMAL, "limits": {"maxTurns": 5, "extra": 1}},
            "unknown setting: limits.extra",
        ),
        ("not an object", "object"),
    ],
)
def test_invalid_configs_name_the_offending_setting(bad, needle):
    with pytest.raises(ConfigError, match=needle):
        parse_config(bad)


def test_config_hash_is_canonical():
    assert config_hash({"b": 1, "a": [1, 2]}) == config_hash({"a": [1, 2], "b": 1})
    assert len(config_hash(MINIMAL)) == 64


def test_parse_task_reads_prompt_inputs_and_upload():
    task = parse_task(
        {
            "text": "Load it",
            "data": {
                "inputs": [{"url": "https://x/a.csv", "path": "data/a.csv"}],
                "upload": {"url": "https://x/put", "headers": {"x-amz-acl": "private"}},
                "ticket": 17,
            },
        }
    )
    assert task.prompt == "Load it" and task.inputs[0].path == "data/a.csv"
    assert task.upload.method == "PUT" and task.upload.headers == (
        ("x-amz-acl", "private"),
    )
    assert parse_task({"text": "Just a prompt"}).upload is None


@pytest.mark.parametrize("path", ["../a.csv", "/etc/passwd", "data/../../x", ""])
def test_parse_task_refuses_paths_outside_the_workspace(path):
    with pytest.raises(ConfigError, match="path"):
        parse_task(
            {"text": "x", "data": {"inputs": [{"url": "https://x/a", "path": path}]}}
        )


def test_parse_task_refuses_non_http_urls_and_missing_prompt():
    with pytest.raises(ConfigError, match="url"):
        parse_task(
            {
                "text": "x",
                "data": {"inputs": [{"url": "file:///etc/passwd", "path": "a"}]},
            }
        )
    with pytest.raises(ConfigError, match="text"):
        parse_task({"data": {}})


def test_parse_bindings_reads_the_ssh_workspace_descriptor():
    access = parse_bindings(
        {
            "connectors": {},
            "workspace": {
                "uid": "u",
                "generation": 1,
                "backend": "sandbox",
                "transport": "ssh",
                "host": "10.42.0.9",
                "port": 30022,
                "user": "agent-host",
                "root": "/home/agent-host/workspace",
                "privateKeyFile": "/run/srw-workspace/id_ed25519",
                "knownHostsFile": "/run/srw-workspace/known_hosts",
                "sshCommand": ["/run/srw-workspace/ssh"],
            },
        }
    )
    assert (access.host, access.port, access.user, access.root) == (
        "10.42.0.9",
        30022,
        "agent-host",
        "/home/agent-host/workspace",
    )
    with pytest.raises(ConfigError, match="workspace"):
        parse_bindings({"connectors": {}})


@pytest.mark.parametrize(("kind", "field"), [("preset", "append"), ("custom", "text")])
def test_system_prompt_texts_are_bounded_in_utf8_bytes(kind, field):
    at_limit = "é" * (65536 // 2)  # two bytes each: exactly 65536 bytes
    config = parse_config({**MINIMAL, "systemPrompt": {"type": kind, field: at_limit}})
    assert config.system_prompt.text == at_limit
    over = at_limit + "x"  # 65537 bytes in only 32769 characters
    with pytest.raises(ConfigError) as error:
        parse_config({**MINIMAL, "systemPrompt": {"type": kind, field: over}})
    message = str(error.value)
    assert f"systemPrompt.{field}" in message and "65536 bytes" in message
    assert "é" not in message


def test_model_is_bounded_and_has_no_whitespace_or_control_characters():
    assert parse_config({**MINIMAL, "model": "m" * 256}).model == "m" * 256
    for bad in (
        "",
        "m" * 257,
        "claude sonnet",
        " claude",
        "claude\t",
        "claude\nx",
        "claude\x00",
        "claude\x7f",
        "claude\u0085",
        "claude ",
        "claude\ud800",
        42,
        None,
    ):
        with pytest.raises(ConfigError) as error:
            parse_config({**MINIMAL, "model": bad})
        message = str(error.value)
        assert message.startswith("model ") and "256 characters" in message
        if isinstance(bad, str) and bad:
            assert bad not in message


def test_thinking_budget_is_bounded():
    for good in (1024, 1_000_000):
        thinking = {"type": "enabled", "budget_tokens": good}
        assert parse_config({**MINIMAL, "thinking": thinking}).thinking == thinking
    for bad in (1023, 1_000_001, 0, -5, 2048.0, True, "4096"):
        with pytest.raises(ConfigError) as error:
            parse_config(
                {**MINIMAL, "thinking": {"type": "enabled", "budget_tokens": bad}}
            )
        message = str(error.value)
        assert "thinking.budget_tokens" in message
        assert "1024" in message and "1000000" in message
        assert repr(bad) not in message.replace("1024", "").replace("1000000", "")
