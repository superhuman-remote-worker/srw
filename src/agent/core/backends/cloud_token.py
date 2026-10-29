"""The bearer of a Virtual Session's cloud folder, fetched per call.

A remote entry of the ``oauth_token`` auth kind
(``features/cloud_folders_on_virtual_sessions.md`` § Provider neutrality)
carries no credential. The executor fetches the folder's short-lived bearer
from the orchestrator's token route under its claim lease
(``OrchestratorClient.cloud_remote_token_fetcher``) and the rclone store adds
it to each spawn's environment (:class:`LiveRemoteCredential`). The token is
cached until shortly before it expires and dropped when the remote refuses it;
no refresh token and no client secret ever reach this process.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Mapping
from typing import Optional

from shared.runtime.core.backends.object_store import ObjectStoreError

#: Fetch a fresh bearer this long before the cached one expires.
DEFAULT_REFRESH_MARGIN_SECONDS = 60.0


class CloudTokenUnavailable(ObjectStoreError):
    """The folder's bearer could not be fetched. The message is a closed
    phrase (a status code, a transport error's name); never a response body."""


class CloudTokenSource:
    """The live credential of one cloud folder: ``config()`` answers rclone's
    ``bearer_token`` option from a cached token, fetching through ``fetch``
    (``() -> (token, expires_in)``) when there is none or it is within
    ``refresh_margin`` seconds of its end; ``rejected()`` drops the cache so
    the next spawn fetches afresh. Safe to call from the worker threads the
    store spawns on."""

    def __init__(
        self,
        fetch: Callable[[], tuple[str, float]],
        *,
        refresh_margin: float = DEFAULT_REFRESH_MARGIN_SECONDS,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._fetch = fetch
        self._refresh_margin = float(refresh_margin)
        self._clock = clock
        self._token: Optional[str] = None
        self._expires_at: float = 0.0
        self._lock = threading.Lock()

    def config(self) -> Mapping[str, str]:
        with self._lock:
            now = self._clock()
            if self._token is None or now >= self._expires_at - self._refresh_margin:
                try:
                    token, expires_in = self._fetch()
                except CloudTokenUnavailable:
                    raise
                except Exception as exc:
                    raise CloudTokenUnavailable(
                        f"cloud folder token unavailable ({type(exc).__name__})"
                    ) from exc
                if not isinstance(token, str) or not token:
                    raise CloudTokenUnavailable("cloud folder token unavailable")
                self._token = token
                self._expires_at = now + max(0.0, float(expires_in))
            return {"bearer_token": self._token}

    def rejected(self) -> None:
        with self._lock:
            self._token = None
            self._expires_at = 0.0

    @property
    def cached(self) -> bool:
        """Whether a token is held right now (for tests and status)."""
        with self._lock:
            return self._token is not None


def unavailable_token_source(reason: str) -> CloudTokenSource:
    """A source for an executor that has no token route (the pinned lane, a
    test): every spawn fails with ``reason`` as a closed message, so the
    folder binds as failed instead of the whole workspace."""

    def refuse() -> tuple[str, float]:
        raise CloudTokenUnavailable(reason)

    return CloudTokenSource(refuse)


__all__ = [
    "DEFAULT_REFRESH_MARGIN_SECONDS",
    "CloudTokenSource",
    "CloudTokenUnavailable",
    "unavailable_token_source",
]
