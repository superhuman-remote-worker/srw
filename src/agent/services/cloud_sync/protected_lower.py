"""The provider half of a protected-cloud lower mount, as the agent accepts it.

The orchestrator's Nextcloud adapter builds it
(``NextcloudBackend.protected_lower_transport``): rclone WebDAV as the
per-mount reader account, with basic auth. No other provider offers the
protected level, so any other transport is refused; the agent never mounts a
protected lower with a credential it cannot tell is the reader's.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def _text(value: Any) -> bool:
    return isinstance(value, str) and bool(value)


def is_protected_reader_transport(lower: Mapping[str, Any]) -> bool:
    """Whether ``lower`` carries exactly the reader transport (fail closed)."""

    source = lower.get("source")
    config = source.get("config") if isinstance(source, Mapping) else None
    auth = lower.get("auth")
    return bool(
        lower.get("backend") == "nextcloud"
        and isinstance(source, Mapping)
        and source.get("type") == "webdav"
        and isinstance(config, Mapping)
        and config.get("vendor") == "nextcloud"
        and _text(config.get("url"))
        and _text(config.get("user"))
        and isinstance(auth, Mapping)
        and auth.get("type") == "basic"
        and _text(auth.get("password"))
    )


__all__ = ["is_protected_reader_transport"]
