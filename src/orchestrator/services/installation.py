"""What this installation is, for a client that must match it."""

from collections.abc import Mapping
import os
from typing import Any

from shared.manifests import API_VERSION


def server_version(environment: Mapping[str, str] | None = None) -> dict[str, Any]:
    """What this installation is, for a client that must match it.

    The release and the source revision are baked into the image at build
    time (``SRW_RELEASE_VERSION``, ``SRW_SOURCE_REVISION``); a process started
    from a checkout has neither and says so with the same words the MCP
    server's health report uses. ``cockpit_url`` is where a person opens what
    the API names (``COCKPIT_EXTERNAL_URL``), null when the installation does
    not say.
    """
    environment = os.environ if environment is None else environment
    cockpit_url = (environment.get("COCKPIT_EXTERNAL_URL") or "").strip().rstrip("/")
    return {
        "release_version": environment.get("SRW_RELEASE_VERSION") or "development",
        "source_revision": environment.get("SRW_SOURCE_REVISION") or "unavailable",
        "manifest_api_versions": [API_VERSION],
        "cockpit_url": cockpit_url or None,
    }
