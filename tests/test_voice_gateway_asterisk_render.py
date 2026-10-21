"""Asterisk addon config renderer; local_voice_gateway.md §4.4.

The golden files under tests/fixtures/voice_gateway_asterisk/expected are the
renderer's output for values.yaml next to them. After a deliberate change,
regenerate them with VOICE_GATEWAY_GOLDEN=update and review the diff.
"""

from __future__ import annotations

import os
import shutil
import stat
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

from voice_gateway.asterisk.render import load_values, main, render, write
from voice_gateway.errors import ConfigError

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests" / "fixtures" / "voice_gateway_asterisk"
EXAMPLE = ROOT / "src" / "voice_gateway" / "asterisk" / "addon.example.yaml"
SECRETS = ("test-asterisk-secret", "test-reception-secret", "test-clinic-secret")


def test_golden_output():
    files = render(load_values(FIXTURES / "values.yaml"))
    expected = FIXTURES / "expected"
    if os.environ.get("VOICE_GATEWAY_GOLDEN") == "update":
        shutil.rmtree(expected, ignore_errors=True)
        expected.mkdir()
        for name, text in files.items():
            (expected / name).write_text(text)
    assert sorted(files) == sorted(p.name for p in expected.iterdir())
    for name, text in files.items():
        assert text == (expected / name).read_text(), name


def test_files_are_private(tmp_path):
    out = tmp_path / "etc"
    write(render(load_values(FIXTURES / "values.yaml")), out)
    assert stat.S_IMODE(out.stat().st_mode) == 0o750
    modes = {stat.S_IMODE(p.stat().st_mode) for p in out.iterdir()}
    assert modes == {0o640}


def test_cli_never_prints_secrets(tmp_path):
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "voice_gateway.asterisk.render",
            "--values",
            str(FIXTURES / "values.yaml"),
            "--out",
            str(tmp_path / "etc"),
        ],
        capture_output=True,
        text=True,
        env={**os.environ, "PYTHONPATH": str(ROOT / "src")},
        check=True,
    )
    output = result.stdout + result.stderr
    assert "pjsip.conf" in output
    assert not any(secret in output for secret in SECRETS)


def test_example_values_render(tmp_path):
    raw = EXAMPLE.read_text()
    for name in ("asterisk-password", "line-reception-password"):
        (tmp_path / name).write_text("example-secret\n")
        raw = raw.replace(f"/run/secrets/{name}", name)
    (tmp_path / "addon.yaml").write_text(raw)
    files = render(load_values(tmp_path / "addon.yaml"))
    assert "[srw-reception]\ntype = registration" in files["pjsip.conf"]
    assert "[srw-hotline]\ntype = identify" in files["pjsip.conf"]


def test_the_contract_with_the_source():
    files = render(load_values(FIXTURES / "values.yaml"))
    dialplan = files["extensions.conf"]
    # Every call reaches the Stasis app with its line's name ...
    assert " same => n,Stasis(srw-voice,reception)\n" in dialplan
    # ... and the transfer context returns with the result.
    assert "exten => s,1,Transfer(PJSIP/${SRW_REFER_TO})" in dialplan
    assert (
        "Stasis(srw-voice,${SRW_LINE},transfer,${TRANSFERSTATUS},"
        "${TRANSFERSTATUSPROTOCOL})" in dialplan
    )
    assert "control_message_format = json" in files["chan_websocket.conf"]
    assert "connection_type = per_call_config" in files["websocket_client.conf"]
    # Nothing can write audio: no file formats, no recordings API.
    for module in ("format_wav.so", "format_pcm.so", "format_sln.so"):
        assert f"noload = {module}" in files["modules.conf"]
    assert "noload = res_ari_recordings.so" in files["modules.conf"]


def values(tmp_path, change) -> Path:
    raw = yaml.safe_load((FIXTURES / "values.yaml").read_text())
    for name in (
        "asterisk-password",
        "line-reception-password",
        "line-clinic-password",
    ):
        shutil.copy(FIXTURES / name, tmp_path / name)
    change(raw)
    path = tmp_path / "values.yaml"
    path.write_text(yaml.safe_dump(raw))
    return path


@pytest.mark.parametrize(
    "change, message",
    [
        (lambda r: r["lines"][0].update(name="Reception"), "line name"),
        (lambda r: r["lines"][0].pop("password_file"), "extension and password_file"),
        (lambda r: r["lines"][0].update(mode="trunk"), "register or peer"),
        (lambda r: r["lines"][0].update(extension="20,00"), "characters"),
        (lambda r: r["lines"][0].update(pbx="pbx example"), "IP address or host name"),
        (lambda r: r["lines"][0].update(codecs=["opus"]), "codecs"),
        (lambda r: r["lines"][0].update(match=["192.168.10.2"]), "registration"),
        (lambda r: r["lines"][0].update(extra=1), "unknown keys"),
        (lambda r: r["lines"][1].update(name="reception"), "unique"),
        (
            lambda r: r["lines"][2].update(match=["192.168.10.3"])
            or r["lines"].append(
                {"name": "other", "mode": "peer", "pbx": "192.168.10.3"}
            ),
            "same address",
        ),
        (lambda r: r["ari"].update(bind="0.0.0.0:8088"), "never every interface"),
        (lambda r: r["sip"].pop("local_net"), "local_net"),
        (lambda r: r["rtp"].update(end=20001), "two ports"),
        (lambda r: r["gateway"].update(media_uri="http://gw/media"), "ws://"),
        (lambda r: r.update(lines=[]), "nonempty"),
    ],
)
def test_invalid_values(tmp_path, change, message):
    with pytest.raises(ConfigError, match=message):
        load_values(values(tmp_path, change))


@pytest.mark.parametrize("secret", ["semi;colon", 'quo"te', "back\\slash", "new\nline"])
def test_unsafe_password_is_refused_without_echo(tmp_path, capsys, secret):
    path = values(tmp_path, lambda r: None)
    (tmp_path / "line-reception-password").write_text(secret)
    assert main(["--values", str(path), "--out", str(tmp_path / "etc")]) == 2
    err = capsys.readouterr().err
    assert "password_file" in err and secret not in err
    assert not (tmp_path / "etc").exists()


def test_ari_bind_from_the_command_line(tmp_path):
    values = str(FIXTURES / "values.yaml")
    out = tmp_path / "etc"
    assert (
        main(["--values", values, "--out", str(out), "--ari-bind", "10.42.1.9:8089"])
        == 0
    )
    http = (out / "http.conf").read_text()
    assert "bindaddr = 10.42.1.9\nbindport = 8089\n" in http
    assert (
        main(["--values", values, "--out", str(out), "--ari-bind", "0.0.0.0:8088"]) == 2
    )
