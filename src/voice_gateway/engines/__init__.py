"""Engine kinds behind the profiles; local_voice_gateway.md §4.3.

A profile in the config names an engine kind and that kind's settings, and
its name is the model id a line's live_voice catalog row sends in accept.
Each kind checks its own settings and returns a factory that makes one engine
per call. A new kind registers itself in ``KINDS``.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping

from voice_gateway.engines.echo import echo_kind
from voice_gateway.errors import ConfigError
from voice_gateway.interfaces import Engine
from voice_gateway.scripted import scripted_kind

EngineFactory = Callable[[], Engine]
EngineKind = Callable[[str, dict], EngineFactory]

KINDS: dict[str, EngineKind] = {
    "scripted": scripted_kind,
    "echo": echo_kind,
}


def build_engines(profiles: Mapping[str, object]) -> dict[str, EngineFactory]:
    """One factory per profile; raises ConfigError for an unusable profile."""
    factories = {}
    for name, profile in profiles.items():
        if not isinstance(profile, dict):
            raise ConfigError(f"profile {name} must be a mapping")
        settings = dict(profile)
        kind = settings.pop("engine", None)
        if kind not in KINDS:
            raise ConfigError(
                f"profile {name}: engine must be one of {', '.join(sorted(KINDS))}"
            )
        factories[name] = KINDS[kind](name, settings)
    return factories
