"""Errors shared by the gateway's modules; local_voice_gateway.md §4.4."""


class ConfigError(ValueError):
    """An invalid operator setting, without secret values in the message."""
