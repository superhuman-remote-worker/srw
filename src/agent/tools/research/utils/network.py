"""Network configuration for research tools.

Provides proxy configuration for routing requests through institutional
networks (university VPN, SSH tunnels, HTTP proxies) to access paywalled content.

Also provides ``research_request()``, a proxy-aware HTTP client with automatic
retry on connection failures (essential for VPN connections that drop periodically).
"""

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from dataclasses import dataclass
from enum import Enum
from typing import Any, AsyncIterator, Dict, Optional

import aiohttp

logger = logging.getLogger(__name__)


class ProxyType(Enum):
    """Supported proxy types."""

    NONE = "none"
    HTTP = "http"
    SOCKS5 = "socks5"


@dataclass
class ProxyConfig:
    """Proxy configuration for routing requests through institutional networks.

    Loaded from environment variables or agent config. Supports HTTP and SOCKS5
    proxies (e.g., SSH tunnels to university networks).

    Usage with SSH tunnel:
        ssh -D 1080 -N user@university.edu
        RESEARCH_PROXY_TYPE=socks5 RESEARCH_PROXY_HOST=localhost RESEARCH_PROXY_PORT=1080
    """

    type: ProxyType = ProxyType.NONE
    host: Optional[str] = None
    port: Optional[int] = None
    username: Optional[str] = None
    password: Optional[str] = None

    @property
    def url(self) -> Optional[str]:
        """Get the proxy URL string (e.g., 'socks5://localhost:1080')."""
        if self.type == ProxyType.NONE or not self.host or not self.port:
            return None
        auth = f"{self.username}:{self.password}@" if self.username else ""
        return f"{self.type.value}://{auth}{self.host}:{self.port}"

    @property
    def is_configured(self) -> bool:
        """Check if a proxy is actually configured and usable."""
        return (
            self.type != ProxyType.NONE
            and self.host is not None
            and self.port is not None
        )

    @classmethod
    def from_env(cls) -> "ProxyConfig":
        """Load proxy config from environment variables.

        Env vars:
            RESEARCH_PROXY_TYPE: "http", "socks5", or "none" (default: "none")
            RESEARCH_PROXY_HOST: Proxy host (e.g., "localhost")
            RESEARCH_PROXY_PORT: Proxy port (e.g., "1080")
            RESEARCH_PROXY_USER: Optional proxy username
            RESEARCH_PROXY_PASS: Optional proxy password
        """
        proxy_type_str = os.getenv("RESEARCH_PROXY_TYPE", "none").lower()

        try:
            proxy_type = ProxyType(proxy_type_str)
        except ValueError:
            logger.warning(
                f"Unknown RESEARCH_PROXY_TYPE: {proxy_type_str}. Using 'none'."
            )
            proxy_type = ProxyType.NONE

        if proxy_type == ProxyType.NONE:
            return cls()

        port_str = os.getenv("RESEARCH_PROXY_PORT")
        port = int(port_str) if port_str else None

        return cls(
            type=proxy_type,
            host=os.getenv("RESEARCH_PROXY_HOST"),
            port=port,
            username=os.getenv("RESEARCH_PROXY_USER"),
            password=os.getenv("RESEARCH_PROXY_PASS"),
        )

    @classmethod
    def from_config(cls, config: Dict[str, Any]) -> "ProxyConfig":
        """Load proxy config from agent config dict.

        Expected format (from config/expert_base.yaml research.proxy section):
            proxy:
              enabled: true
              type: socks5
              host: localhost
              port: 1080

        Falls back to environment variables if not in config.

        Args:
            config: Dict with proxy configuration fields
        """
        if not config or not config.get("enabled", False):
            # Try env vars as fallback
            return cls.from_env()

        proxy_type_str = config.get("type", "none")
        try:
            proxy_type = ProxyType(proxy_type_str)
        except ValueError:
            logger.warning(f"Unknown proxy type in config: {proxy_type_str}")
            proxy_type = ProxyType.NONE

        return cls(
            type=proxy_type,
            host=config.get("host"),
            port=config.get("port"),
            username=config.get("username") or os.getenv("RESEARCH_PROXY_USER"),
            password=config.get("password") or os.getenv("RESEARCH_PROXY_PASS"),
        )

    def to_aiohttp_connector(self) -> Optional[Any]:
        """Create an aiohttp-socks ProxyConnector for this proxy config.

        Returns:
            ProxyConnector instance, or None if not configured or
            aiohttp-socks is not installed.
        """
        if not self.is_configured:
            return None

        try:
            from aiohttp_socks import ProxyConnector, ProxyType as SocksProxyType

            proxy_type_map = {
                ProxyType.SOCKS5: SocksProxyType.SOCKS5,
                ProxyType.HTTP: SocksProxyType.HTTP,
            }
            socks_type = proxy_type_map.get(self.type)
            if socks_type is None:
                return None

            return ProxyConnector(
                proxy_type=socks_type,
                host=self.host,
                port=self.port,
                username=self.username,
                password=self.password,
                rdns=True,
            )
        except ImportError:
            logger.warning(
                "aiohttp-socks not installed. Install with: pip install aiohttp-socks. "
                "Proxy will be ignored for HTTP requests."
            )
            return None


# Connection-level errors worth retrying (proxy down, VPN drop, network blip)
_RETRIABLE_ERRORS = (
    aiohttp.ClientConnectorError,
    aiohttp.ServerDisconnectedError,
    aiohttp.ClientOSError,
    asyncio.TimeoutError,
    ConnectionError,
    OSError,
)


@asynccontextmanager
async def research_request(
    method: str,
    url: str,
    *,
    proxy: Optional[ProxyConfig] = None,
    timeout: float = 30,
    max_retries: int = 3,
    **request_kwargs,
) -> AsyncIterator[aiohttp.ClientResponse]:
    """Make an HTTP request with optional proxy routing and retry on connection errors.

    Standard way for research tools to make aiohttp requests.  Routes through
    the configured SOCKS5/HTTP proxy (if any) and retries on connection-level
    failures with exponential backoff — essential for VPN connections that may
    drop periodically.

    HTTP-level errors (429, 404, etc.) are NOT retried.  The response is
    yielded so callers keep their existing status-code handling.

    Usage::

        async with research_request("GET", url, proxy=proxy, params=params) as resp:
            if resp.status == 429:
                return "rate limited"
            data = await resp.json()

    Args:
        method: HTTP method ("GET", "HEAD", "POST", etc.)
        url: Request URL.
        proxy: ProxyConfig instance, or None for direct connection.
        timeout: Request timeout in seconds (default 30).
        max_retries: Maximum retry attempts on connection failure (default 3).
        **request_kwargs: Passed to ``session.request()`` (params, headers, etc.)

    Yields:
        aiohttp.ClientResponse

    Raises:
        ConnectionError: All retries exhausted due to connection/proxy failures.
            Message includes proxy host/port when applicable.
    """
    use_proxy = proxy and proxy.is_configured
    client_timeout = aiohttp.ClientTimeout(total=timeout)
    last_error: Optional[BaseException] = None

    for attempt in range(1, max_retries + 1):
        connector = None
        session = None
        if use_proxy:
            connector = proxy.to_aiohttp_connector()
            if connector and attempt == 1:
                logger.debug(
                    f"Research request via proxy: "
                    f"{proxy.type.value}://{proxy.host}:{proxy.port}"
                )

        try:
            session = aiohttp.ClientSession(connector=connector, timeout=client_timeout)
            resp = await session.request(method, url, **request_kwargs)
            # Connection succeeded — yield response to caller
            try:
                yield resp
            finally:
                resp.release()
                await session.close()
            return  # Caller consumed response successfully

        except _RETRIABLE_ERRORS as e:
            last_error = e
            if session:
                await session.close()

            if attempt < max_retries:
                delay = min(2 ** (attempt - 1), 8)
                logger.warning(
                    f"Research request to {url} failed "
                    f"(attempt {attempt}/{max_retries}): "
                    f"{type(e).__name__}: {e}. Retrying in {delay:.0f}s..."
                )
                await asyncio.sleep(delay)
            else:
                logger.error(
                    f"Research request to {url} failed after "
                    f"{max_retries} attempts: {type(e).__name__}: {e}"
                )

        except GeneratorExit:
            if session:
                await session.close()
            return

    # All retries exhausted
    proxy_hint = ""
    if use_proxy:
        proxy_hint = (
            f" Proxy: {proxy.type.value}://{proxy.host}:{proxy.port}."
            f" Check that the VPN/proxy is running and accessible."
        )
    raise ConnectionError(
        f"Failed to connect to {url} after {max_retries} attempts. "
        f"Last error: {last_error}.{proxy_hint}"
    )


def get_proxy_from_context(context) -> ProxyConfig:
    """Extract ProxyConfig from a ToolContext's config dict.

    Args:
        context: ToolContext with ``config`` dict containing ``research.proxy``.

    Returns:
        ProxyConfig (unconfigured if proxy not enabled in config or env).
    """
    proxy_data = context.config.get("research", {}).get("proxy", {})
    return ProxyConfig.from_config(proxy_data)
