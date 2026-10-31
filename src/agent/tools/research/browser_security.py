"""Browser security utilities — URL validation and content nonce wrapping.

Blocks dangerous schemes, cloud metadata endpoints (by name, and by literal
address in any spelling: link-local, metadata, multicast and reserved ranges,
through :mod:`shared.url_safety`), and K8s internal DNS. Content nonces
prevent prompt injection from page content.

Note: Private IPs and localhost are intentionally allowed. The browser runs
inside the agent's isolated workspace container, so localhost refers to that
container — not the host or orchestrator. Blocking it would prevent the
agent from testing any web application it builds. Names are not resolved
here (the workspace resolves them, not the agent pod); the workspace's
NetworkPolicy is the gate for a name that points somewhere private.
Server-side fetches from the agent pod use the stricter
``shared.url_safety.check_public_http_url``.
"""

import logging
import re
import secrets
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from shared.url_safety import address_refusal, literal_ip, url_host

logger = logging.getLogger(__name__)

# ── URL Validation ───────────────────────────────────────────────────

# Hostnames that resolve to cloud metadata endpoints
_BLOCKED_HOSTNAMES = {
    "metadata.google.internal",
    "metadata.goog",
}

# K8s internal DNS pattern
_K8S_INTERNAL_RE = re.compile(
    r"\.(cluster\.local|svc|svc\.cluster\.local|pod|pod\.cluster\.local)$",
    re.IGNORECASE,
)

# Default blocked schemes
_DEFAULT_BLOCKED_SCHEMES = {"file", "javascript", "data"}


def validate_url(
    url: str,
    allowed_domains: Optional[List[str]] = None,
    blocked_domains: Optional[List[str]] = None,
    blocked_schemes: Optional[List[str]] = None,
) -> str:
    """Validate a URL for browser navigation. Returns the cleaned URL.

    Raises:
        ValueError: If the URL is blocked by security policy.
    """
    if not url or not url.strip():
        raise ValueError("URL cannot be empty")

    parsed = urlparse(url.strip())

    # Scheme check
    scheme = (parsed.scheme or "").lower()
    if not scheme:
        # Default to https if no scheme
        url = f"https://{url.strip()}"
        parsed = urlparse(url)
        scheme = "https"

    schemes_to_block = (
        set(blocked_schemes) if blocked_schemes else _DEFAULT_BLOCKED_SCHEMES
    )
    if scheme in schemes_to_block:
        raise ValueError(f"Blocked scheme: {scheme}://")

    if scheme not in ("http", "https"):
        raise ValueError(f"Unsupported scheme: {scheme}://")

    # The host Chromium will dial: a backslash ends the authority for it
    # (``http://169.254.169.254\@example.com/``), not for urlparse.
    try:
        hostname = (url_host(url.strip()).hostname or "").lower()
    except ValueError as exc:
        raise ValueError(f"Invalid URL: {exc}") from exc
    if not hostname:
        raise ValueError("URL has no hostname")

    # Blocked hostnames (cloud metadata endpoints)
    if hostname in _BLOCKED_HOSTNAMES:
        raise ValueError(f"Blocked hostname: {hostname}")

    # K8s internal DNS
    if _K8S_INTERNAL_RE.search(hostname):
        raise ValueError(f"Blocked K8s internal hostname: {hostname}")

    # Domain allowlist (if configured, only these are permitted)
    if allowed_domains:
        if not any(
            hostname == d or hostname.endswith(f".{d}") for d in allowed_domains
        ):
            raise ValueError(
                f"Domain {hostname} not in allowed list: {allowed_domains}"
            )

    # Domain blocklist (for explicit per-config restrictions)
    if blocked_domains:
        for d in blocked_domains:
            if hostname == d or hostname.endswith(f".{d}"):
                raise ValueError(f"Blocked domain: {hostname}")

    # A literal address in any spelling (169.254.169.254, 2852039166,
    # [::ffff:169.254.169.254]) in the never-dialled ranges: link-local and
    # cloud metadata, multicast, reserved. Loopback and private addresses stay
    # allowed (see the module docstring), and so does the unspecified address,
    # which reaches the workspace's own listeners the way a dev server
    # announces them ("listening on 0.0.0.0:3000").
    literal = literal_ip(hostname)
    if literal is not None and not literal.is_unspecified:
        reason = address_refusal(literal, private_allowed=True)
        if reason is not None:
            raise ValueError(f"Blocked address: {hostname} {reason}")

    return url.strip()


def get_security_config(browser_config: Dict[str, Any]) -> Dict[str, Any]:
    """Extract security settings from browser config with defaults."""
    security = browser_config.get("security", {})
    return {
        "allowed_domains": security.get("allowed_domains", []),
        "blocked_domains": security.get("blocked_domains", []),
        "blocked_schemes": security.get(
            "blocked_schemes", ["file", "javascript", "data"]
        ),
    }


def validate_url_with_config(url: str, browser_config: Dict[str, Any]) -> str:
    """Validate URL using the browser config's security settings."""
    sec = get_security_config(browser_config)
    return validate_url(
        url,
        allowed_domains=sec["allowed_domains"] or None,
        blocked_domains=sec["blocked_domains"] or None,
        blocked_schemes=sec["blocked_schemes"] or None,
    )


# ── Content Nonce Wrapping ───────────────────────────────────────────


def generate_nonce() -> str:
    """Generate a CSPRNG nonce for content boundaries."""
    return secrets.token_hex(16)


def wrap_with_nonce(content: Dict[str, Any]) -> Dict[str, Any]:
    """Wrap page content with nonce boundaries to prevent prompt injection.

    The nonce is unpredictable — malicious pages cannot forge boundaries.
    The agent sees the nonce markers and knows everything between them is
    untrusted page content.

    Args:
        content: Dict with keys like 'dom', 'screenshot', 'url', 'title'.

    Returns:
        Same dict with 'dom' wrapped in nonce boundaries and 'nonce' key added.
    """
    nonce = generate_nonce()
    result = dict(content)
    result["nonce"] = nonce

    if "dom" in result and result["dom"]:
        result["dom"] = (
            f"[page_content nonce={nonce}]\n"
            f"{result['dom']}\n"
            f"[/page_content nonce={nonce}]"
        )

    return result
