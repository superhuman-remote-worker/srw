"""The ``cascade`` engine kind; knowledge-base/knowledge/features/local_voice_gateway.md §4.3.

Turn detection → STT → voice LLM → TTS on Pipecat, each stage an
OpenAI-compatible server. Settings are checked at config load without
importing Pipecat; the pipeline modules load when the first call starts.
Pipecat and its models come from the gateway's own requirements
(``src/voice_gateway/requirements.txt``), not the project's.
"""

from __future__ import annotations

import importlib.util
from collections.abc import Callable
from pathlib import Path

from voice_gateway.engines.cascade.settings import CascadeSettings, parse_settings
from voice_gateway.errors import ConfigError

__all__ = ["CascadeSettings", "cascade_kind", "parse_settings"]


def cascade_kind(name: str, settings: dict, base: Path) -> Callable[[], object]:
    """The ``cascade`` profile kind: validate settings, return a per-call factory."""
    parsed = parse_settings(name, settings, base)
    if importlib.util.find_spec("pipecat") is None:
        raise ConfigError(
            f"profile {name}: the cascade engine needs the gateway's voice "
            "requirements (pipecat-ai) installed"
        )

    def factory():
        from voice_gateway.engines.cascade.engine import CascadeEngine

        return CascadeEngine(parsed)

    return factory
