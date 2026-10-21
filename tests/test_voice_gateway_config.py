"""Gateway config and signer boundaries; local_voice_gateway.md §4.1, §4.4."""

import base64
import json
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

from shared.runtime.live.webhook import sign_webhook as runtime_sign
from shared.runtime.live.webhook import verify_webhook
from voice_gateway.config import ConfigError, load_config
from voice_gateway.server import sign_webhook

PYTHON = sys.executable


@pytest.fixture
def config_file(tmp_path):
    (tmp_path / "key").write_text("test-api-key\n")
    (tmp_path / "webhook").write_text(
        "whsec_" + base64.b64encode(b"test-signing-key").decode()
    )
    raw = {
        "listen": "127.0.0.1:18440",
        "api_key_files": ["key"],
        "orchestrator_url": "http://127.0.0.1:8085",
        "lines": [
            {
                "name": "demo",
                "connector_id": "connector",
                "webhook_secret_file": "webhook",
            }
        ],
        "profiles": {"scripted": {"engine": "scripted"}},
    }
    path = tmp_path / "gateway.yaml"
    path.write_text(yaml.safe_dump(raw))
    return path, raw


def test_load_relative_secret_files_and_defaults(config_file):
    path, _ = config_file
    config = load_config(path)
    assert (config.host, config.port) == ("127.0.0.1", 18440)
    assert not config.test_control
    assert config.webhook_verify_ssl
    assert config.api_keys == (b"test-api-key",)
    assert config.lines["demo"].webhook_key == b"test-signing-key"
    assert "test-api-key" not in repr(config)
    assert "test-signing-key" not in repr(config)


@pytest.mark.parametrize(
    "change, message",
    [
        ({"listen": "0.0.0.0:18440", "test_control": True}, "requires loopback"),
        ({"listen": "hostname.invalid:18440"}, "listen must"),
        ({"listen": "127.0.0.1:0"}, "listen must"),
        ({"listen": "127.0.0.1:65536"}, "listen must"),
        ({"api_key_files": []}, "api_key_files"),
        ({"api_key_files": ["missing"]}, "cannot read API"),
        ({"lines": []}, "lines must"),
        ({"lines": [None]}, "each line"),
        ({"profiles": {}}, "profiles must"),
        ({"profiles": {"bad": {"engine": "unknown"}}}, "engine must be one of"),
        ({"profiles": {"bad": {"engine": "scripted", "x": 1}}}, "takes no settings"),
        ({"profiles": {"bad": None}}, "must be a mapping"),
        ({"source": "asterisk"}, "source must"),
        ({"test_control": "false"}, "true or false"),
        ({"allow_remote_test_control": "yes"}, "true or false"),
        ({"webhook_verify_ssl": "no"}, "true or false"),
        ({"orchestrator_url": "ftp://example.com"}, "http(s)"),
        ({"orchestrator_url": "http://user:password@example.com"}, "http(s)"),
        ({"unexpected": "private-value"}, "unknown fields"),
    ],
)
def test_invalid_config(config_file, change, message):
    path, raw = config_file
    raw.update(change)
    path.write_text(yaml.safe_dump(raw))
    with pytest.raises(
        ConfigError, match=message.replace("(", r"\(").replace(")", r"\)")
    ):
        load_config(path)


@pytest.mark.parametrize(
    "listen, allow",
    [("[::1]:18440", False), ("localhost:18440", False), ("0.0.0.0:18440", True)],
)
def test_test_control_binding(config_file, listen, allow):
    path, raw = config_file
    raw.update(listen=listen, test_control=True, allow_remote_test_control=allow)
    path.write_text(yaml.safe_dump(raw))
    assert load_config(path).test_control


@pytest.mark.parametrize("contents", ["", "whsec_", "whsec_invalid!", "raw-secret"])
def test_invalid_secret_is_never_echoed(config_file, contents):
    path, _ = config_file
    (path.parent / "webhook").write_text(contents)
    with pytest.raises(ConfigError) as exc:
        load_config(path)
    if contents and contents != "whsec_":
        assert contents not in str(exc.value)


def test_duplicate_lines_and_connectors(config_file):
    path, raw = config_file
    raw["lines"].append(raw["lines"][0])
    path.write_text(yaml.safe_dump(raw))
    with pytest.raises(ConfigError, match="unique"):
        load_config(path)


def test_bad_yaml_does_not_echo_contents(config_file):
    path, _ = config_file
    path.write_text("api_key_files: [secret-value\n")
    with pytest.raises(ConfigError) as exc:
        load_config(path)
    assert "secret-value" not in str(exc.value)


def test_cli_invalid_config_has_clear_error(config_file):
    path, _ = config_file
    path.write_text("null")
    result = subprocess.run(
        [PYTHON, "-m", "voice_gateway", "--config", str(path)],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 2
    assert "config must be a mapping" in result.stderr


def test_signer_compatible_with_runtime_verifier():
    key = b"test-signing-key"
    secret = "whsec_" + base64.b64encode(key).decode()
    body = json.dumps(
        {"type": "live.transport.incoming", "data": {"type": "sip"}}
    ).encode()
    timestamp = 1800000000
    signature = sign_webhook(key, "wh_example", timestamp, body)
    assert signature == runtime_sign(secret, "wh_example", timestamp, body)
    assert verify_webhook(
        secret,
        {
            "webhook-id": "wh_example",
            "webhook-timestamp": str(timestamp),
            "webhook-signature": signature,
        },
        body,
        now=timestamp,
    ) == json.loads(body)


def test_gateway_does_not_import_application_runtime():
    root = Path(__file__).resolve().parents[1] / "src" / "voice_gateway"
    for path in root.glob("*.py"):
        assert "from shared.runtime" not in path.read_text()
        assert "from agent" not in path.read_text()
        assert "from orchestrator" not in path.read_text()
