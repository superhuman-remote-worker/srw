"""ChatAnthropic whose HTTP clients trust exactly the endpoint's pinned certificate.

langchain-anthropic has no ``http_client`` field. ``ChatAnthropic._client``
and ``_async_client`` (cached properties) take their httpx client from an
``lru_cache`` keyed by base URL, timeout and proxy, so a pinned and an
unpinned instance on one URL would share one client. This subclass overrides
both properties to build a fresh client per instance whose transport trusts
only the pin (``shared/tls_pinning.py``), and never reads or fills that cache.
Everything else (base URL, timeout, proxy, client class) is built as upstream
builds it.

The transport is of the package the anthropic SDK is built on: httpx up to
0.x, ``httpx2`` from 1.0, which refuses httpx objects.

It rests on private langchain-anthropic API: the ``_client``,
``_async_client`` and ``_client_params`` cached properties and the client
wrappers in ``_client_utils``, the same in 1.4.8, 1.7.1 and 1.7.10. The import
refuses a release where those properties are gone (an override of a property
upstream no longer uses would leave the model unpinned), and
``tests/test_create_llm_tls_pin.py`` makes a real pinned handshake.
"""

from __future__ import annotations

from functools import cached_property
from typing import Any

import anthropic
from langchain_anthropic import ChatAnthropic
from langchain_anthropic._client_utils import (
    _AsyncHttpxClientWrapper,
    _SyncHttpxClientWrapper,
)
from pydantic import PrivateAttr

from shared.tls_pinning import TlsPin, pinned_transport_for

for _name in ("_client", "_async_client", "_client_params"):
    if not isinstance(ChatAnthropic.__dict__.get(_name), cached_property):
        raise ImportError(
            f"langchain-anthropic's ChatAnthropic.{_name} is no longer a cached "
            "property; PinnedChatAnthropic can't pin its client"
        )


class PinnedChatAnthropic(ChatAnthropic):
    """A ``ChatAnthropic`` on a pinned endpoint (built by ``create_llm``)."""

    _tls_pin: TlsPin = PrivateAttr()

    def __init__(self, *, tls_pin: TlsPin, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._tls_pin = tls_pin

    def _pinned_http_client(self, client_cls: type) -> Any:
        """A new ``client_cls`` whose transport trusts only the pin.

        Mirrors ``ChatAnthropic._client``: base URL, timeout and proxy from
        the same parameters, so only the trust differs.
        """
        client_params = self._client_params
        transport_kwargs: dict[str, Any] = {}
        if self.anthropic_proxy:
            transport_kwargs["proxy"] = self.anthropic_proxy
        http_client_params: dict[str, Any] = {
            "base_url": client_params["base_url"],
            "transport": pinned_transport_for(
                client_cls, self._tls_pin, **transport_kwargs
            ),
        }
        if "timeout" in client_params:
            http_client_params["timeout"] = client_params["timeout"]
        return client_cls(**http_client_params)

    @cached_property
    def _client(self) -> anthropic.Client:
        http_client = self._pinned_http_client(_SyncHttpxClientWrapper)
        return anthropic.Client(**self._client_params, http_client=http_client)

    @cached_property
    def _async_client(self) -> anthropic.AsyncClient:
        http_client = self._pinned_http_client(_AsyncHttpxClientWrapper)
        return anthropic.AsyncClient(**self._client_params, http_client=http_client)
