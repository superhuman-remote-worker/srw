"""Call-source kinds; knowledge-base/knowledge/features/local_voice_gateway.md §4.2.

The config's ``source`` names a kind and that kind's settings. Each kind
checks its settings (secret files relative to the config file) and returns a
factory for the gateway's one source. A source that keeps connections of its
own may define ``start(gateway)`` and ``stop()``; the gateway runs them with
its app.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from pathlib import Path

from voice_gateway.asterisk.source import asterisk_kind
from voice_gateway.errors import ConfigError
from voice_gateway.interfaces import CallSource
from voice_gateway.scripted import ScriptedCallSource

SourceFactory = Callable[[], CallSource]


def scripted_kind(settings: dict, base: Path) -> SourceFactory:
    """No physical calls; the test-control API creates them."""
    if settings:
        raise ConfigError("the scripted source takes no settings")
    return ScriptedCallSource


KINDS: dict[str, Callable[[dict, Path], SourceFactory]] = {
    "scripted": scripted_kind,
    "asterisk": asterisk_kind,
}


def build_source(settings: Mapping[str, object], base: Path) -> SourceFactory:
    """The source's factory; raises ConfigError for unusable settings."""
    settings = dict(settings)
    kind = settings.pop("kind", None)
    if kind not in KINDS:
        raise ConfigError(f"source.kind must be one of {', '.join(sorted(KINDS))}")
    return KINDS[kind](settings, base)
