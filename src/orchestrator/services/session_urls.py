"""Public session transport URLs, separate from Kubernetes route selectors."""

from urllib.parse import urlsplit


def session_websocket_origin(public_origin: str, ingress_host: str) -> str:
    """Use the configured HTTPS authority, retaining the legacy host fallback."""
    if not public_origin:
        return f"wss://{ingress_host}"
    try:
        origin = urlsplit(public_origin)
        valid = (
            origin.scheme == "https"
            and origin.hostname
            and origin.username is None
            and origin.password is None
            and origin.path in {"", "/"}
            and not origin.query
            and not origin.fragment
            and (origin.port is None or 1 <= origin.port <= 65535)
            and not any(char.isspace() for char in public_origin)
        )
    except ValueError:
        valid = False
    if not valid:
        raise ValueError(
            "SESSION_PUBLIC_ORIGIN must be an HTTPS origin without credentials, path, query or fragment"
        )
    return f"wss://{origin.netloc}"
