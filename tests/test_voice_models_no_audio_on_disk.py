"""A5 for the cascade's model servers (local_voice_gateway.md §5): what the
caller says and hears may pass through a temporary file only in memory."""

from __future__ import annotations

import re
from pathlib import Path

import yaml

MODELS = Path(__file__).resolve().parent.parent / "docker" / "voice-models"


def test_piper_server_keeps_utterances_in_memory():
    server = (MODELS / "piper_server.py").read_text(encoding="utf-8")
    default = re.search(r'os\.environ\.get\("PIPER_WORK", "([^"]+)"\)', server)
    assert default and default.group(1).startswith("/dev/shm/")
    image = (MODELS / "Dockerfile.piper").read_text(encoding="utf-8")
    work = re.search(r"PIPER_WORK=(\S+)", image)
    assert work and work.group(1).startswith("/dev/shm/")


def test_speech_server_spills_uploads_to_memory():
    compose = yaml.safe_load((MODELS / "compose.yaml").read_text(encoding="utf-8"))
    speech = compose["services"]["speech"]
    assert "/tmp" in speech.get("tmpfs", [])
    # Its HOME (caches, config) is the same tmpfs.
    assert speech["environment"]["HOME"] == "/tmp"
