"""A cookie jar that never stores a cookie, for service clients of Nextcloud.

Nextcloud answers every Basic-auth request with session cookies. Its session
cookies are ``Secure`` (the chart sets ``overwriteprotocol=https``) while SRW
calls it over in-cluster ``http``, so a normal jar sends back only the one
non-secure cookie, ``cookie_test``. For Nextcloud that marks a cookie-capable
client, and it mints a fresh session token on every request: two extra Argon2
runs, an RSA key pair and an ``oc_authtoken`` INSERT, about twice the cost of
the login itself (upstream server issue #27603). A client that sends no cookie
at all pays only the login.

Pass :class:`NoCookieJar` as ``httpx.Client(cookies=...)`` /
``httpx.AsyncClient(cookies=...)``, or assign it to ``requests.Session.cookies``.
Both libraries store response cookies through ``CookieJar.extract_cookies``,
which calls :meth:`set_cookie`, so the jar stays empty and no request carries a
``Cookie`` header. A cookie *policy* would not do this: both libraries copy the
jar into a default-policy jar before sending.

Pure stdlib, so the orchestrator and the agent share it.
"""

from __future__ import annotations

from http.cookiejar import Cookie, CookieJar


class NoCookieJar(CookieJar):
    """A :class:`CookieJar` that drops every cookie it is asked to store."""

    def set_cookie(self, cookie: Cookie) -> None:
        return None
