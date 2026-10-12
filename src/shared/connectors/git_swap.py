"""The git swap driver's shared rules (connector drivers C3).

A ``repository`` connector with token auth reaches its upstream through
``srw.git-swap/v1``: a git smart-HTTP reverse proxy in a service pod that
exchanges the workspace's lease token for the forge token per request. The
orchestrator, the agent and the driver itself (``drivers/git-swap``, Go)
must agree on three things, kept here:

* which repository URLs the driver serves (:func:`swap_upstream`): HTTPS on
  port 443, no credentials in the URL, a plain repository path. Anything
  else stays on the installation's fallback (``connectors.drivers.gitSwap.
  fallback``);
* the path a workspace reaches one connector's repository under:
  ``/<connector id>/<repository path>`` on the driver's endpoint, so the
  driver maps it to the connector's configured upstream and never takes a
  host from the request (:func:`driver_repository_url`);
* where the workspace's git wiring lives (:data:`WIRING_DIR`): under
  ``~/.srw-credentials/``, which no snapshot captures.

``drivers/git-swap/testdata/upstream_vectors.json`` holds the cases both
this module and the driver are tested against.

Design: knowledge-base/knowledge/features/connector_drivers.md, "The git
swap driver".
"""

from __future__ import annotations

import ipaddress
import re
from dataclasses import dataclass
from urllib.parse import urlsplit

#: The one upstream port v1 serves (the driver's declared egress).
UPSTREAM_PORT = 443
#: The workspace's git wiring, relative to its home: the include git reads
#: (``config``), one include per binding (``bindings/``), the credential
#: helper and the certificate authority each binding trusts.
WIRING_DIR = ".srw-credentials/git"
#: What ``~/.gitconfig`` includes (git expands the ``~``).
WIRING_INCLUDE = f"~/{WIRING_DIR}/config"
#: How a fallback is chosen when a token repository cannot use the driver:
#: the token in the clone URL (the behaviour before C3), or no delivery.
FALLBACK_TOKEN_IN_URL = "token-in-url"
FALLBACK_REFUSE = "refuse"
FALLBACKS: tuple[str, ...] = (FALLBACK_TOKEN_IN_URL, FALLBACK_REFUSE)
#: The orchestrator's internal route an agent asks, with a binding's lease
#: token, whether that binding's driver pod was refused: a first clone stops
#: waiting for a pod that will not start (``{"state": "refused", "reason":
#: <a fixed reason>, "retry_in_seconds": <until the reconciler may start it
#: again>}``; otherwise ``waiting`` or ``unknown``).
DRIVER_STATE_PATH = "/api/agents/git-swap/driver-state"
DRIVER_REFUSED = "refused"
DRIVER_WAITING = "waiting"
DRIVER_UNKNOWN = "unknown"

_SEGMENT = re.compile(r"[A-Za-z0-9._~-]{1,128}\Z")
_HOSTNAME = re.compile(
    r"(?=.{1,253}\Z)[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?"
    r"(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)*\Z"
)
_CONNECTOR_ID = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\Z"
)
_MAX_SEGMENTS = 16


class UnservedUpstream(ValueError):
    """A repository URL the git swap driver does not serve; the message says
    why, in words a connector's reader understands."""


@dataclass(frozen=True)
class SwapUpstream:
    """A repository URL the driver serves.

    ``url`` is the URL as the connector names it, cleaned (lowercase scheme
    and host, no credentials, no trailing slash); ``base`` is it without a
    trailing ``.git``; ``remote`` is the one form a swap checkout's origin
    keeps, ``<base>.git``, and the exact string a workspace's ``insteadOf``
    rewrites (so ``o/r.git`` never rewrites ``o/r-docs.git``); ``path`` is
    the repository path the driver compares a request against (no leading
    slash, no ``.git``).
    """

    url: str
    host: str
    path: str

    @property
    def base(self) -> str:
        return self.url.removesuffix(".git")

    @property
    def remote(self) -> str:
        return f"{self.base}.git"


def swap_upstream(url: object) -> SwapUpstream:
    """The repository ``url`` names, or :class:`UnservedUpstream`."""
    if not isinstance(url, str) or not url.strip():
        raise UnservedUpstream("the connector has no repository URL")
    text = url.strip()
    if any(ch.isspace() or ord(ch) < 0x20 or ch in "\\\"'`" for ch in text):
        raise UnservedUpstream("the repository URL has characters it may not have")
    parts = urlsplit(text)
    if parts.scheme.lower() != "https":
        raise UnservedUpstream("the repository URL is not HTTPS")
    if "@" in parts.netloc:
        raise UnservedUpstream("the repository URL carries credentials")
    if "?" in text or "#" in text:
        raise UnservedUpstream("the repository URL has a query or a fragment")
    try:
        port = parts.port
    except ValueError:
        raise UnservedUpstream("the repository URL's port is invalid") from None
    if port not in (None, UPSTREAM_PORT):
        raise UnservedUpstream(f"the repository is not on port {UPSTREAM_PORT}")
    host = (parts.hostname or "").lower()
    if not _host(host):
        raise UnservedUpstream("the repository URL has no host the driver can pin")
    segments = parts.path.strip("/").split("/") if parts.path.strip("/") else []
    if parts.path.startswith("//") or "//" in parts.path.strip("/"):
        raise UnservedUpstream("the repository path has an empty segment")
    if not segments or len(segments) > _MAX_SEGMENTS:
        raise UnservedUpstream("the repository URL names no repository path")
    for segment in segments:
        if not _SEGMENT.fullmatch(segment) or segment in (".", ".."):
            raise UnservedUpstream("the repository path has a segment it may not have")
    path = "/".join(segments)
    stripped = path.removesuffix(".git")
    if not stripped or stripped.endswith("/") or stripped.split("/")[-1] in ("", "."):
        raise UnservedUpstream("the repository URL names no repository path")
    return SwapUpstream(url=f"https://{host}/{path}", host=host, path=stripped)


def _host(host: str) -> bool:
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        return bool(_HOSTNAME.fullmatch(host))
    # A literal address is pinned as given; IPv6 literals are not served.
    return address.version == 4


def connector_path_id(connector_id: object) -> str:
    """The connector id as the driver's path carries it (lowercase)."""
    text = str(connector_id or "").strip().lower()
    if not _CONNECTOR_ID.fullmatch(text):
        raise ValueError("a git swap binding is named by its connector UUID")
    return text


def driver_repository_url(
    endpoint: str, connector_id: object, upstream: SwapUpstream
) -> str:
    """Where a workspace reaches one connector's repository on the driver:
    ``<endpoint>/<connector id>/<repository path>`` (no ``.git``; the clone's
    own suffix follows ``insteadOf``)."""
    if not endpoint.startswith("https://") or endpoint.endswith("/"):
        raise ValueError("the driver endpoint is an https URL without a path")
    return f"{endpoint}/{connector_path_id(connector_id)}/{upstream.path}"


__all__ = [
    "DRIVER_REFUSED",
    "DRIVER_STATE_PATH",
    "DRIVER_UNKNOWN",
    "DRIVER_WAITING",
    "FALLBACKS",
    "FALLBACK_REFUSE",
    "FALLBACK_TOKEN_IN_URL",
    "SwapUpstream",
    "UPSTREAM_PORT",
    "UnservedUpstream",
    "WIRING_DIR",
    "WIRING_INCLUDE",
    "connector_path_id",
    "driver_repository_url",
    "swap_upstream",
]
