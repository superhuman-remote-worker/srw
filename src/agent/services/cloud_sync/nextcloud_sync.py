"""Nextcloud workspace sync — WebDAV + HTTP basic auth."""

from __future__ import annotations

import asyncio
import logging
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Optional
from urllib.parse import unquote, urlparse

from agent.services.cloud_sync.base import (
    CloudSyncFenceLost,
    WorkspaceSyncBase,
    _normalize_dav_listing,
)
from shared.http_no_cookies import NoCookieJar

if TYPE_CHECKING:
    from shared.runtime.core.workspace_backend import WorkspaceBackend

logger = logging.getLogger(__name__)

# Minimal prop set for the one-shot tree listing — exactly what the sync
# algorithm consumes. Requesting allprop makes Nextcloud compute permissions/
# share props per node, for nothing.
_TREE_PROPFIND_BODY = (
    '<?xml version="1.0" encoding="utf-8"?>'
    '<d:propfind xmlns:d="DAV:">'
    "<d:prop><d:getetag/><d:getcontentlength/><d:resourcetype/></d:prop>"
    "</d:propfind>"
)

# Answers that say this server will never serve the tree in one PROPFIND: it
# rejects the request outright (400), does not allow or implement the method
# here (405, 501), or refuses Depth: infinity the RFC 4918 9.1 way (403
# naming the ``propfind-finite-depth`` precondition, see
# _refuses_depth_infinity). A 401, a 404 (the folder is created by the first
# push), 423/429 or any other 5xx is about this moment, not this server.
_DEPTH_INFINITY_REFUSALS = frozenset({400, 405, 501})


def _refuses_depth_infinity(status: int, body: str) -> bool:
    if status in _DEPTH_INFINITY_REFUSALS:
        return True
    # A bare 403 is a permission answer that can change; only the named
    # precondition is a capability verdict.
    return status == 403 and "propfind-finite-depth" in body


def _precondition_headers(if_match: Optional[str], if_none_match: bool) -> list[str]:
    """RFC 4918 preconditions as webdav3 ``headers_ext`` entries.

    ``If-Match`` on a known etag and ``If-None-Match: *`` for a believed-absent
    path are the hard fence of commit-then-effects: a stale push owner whose
    DB token was bumped can still have writes in flight (at most
    ``CLOUD_PUSH_PARALLEL_UPLOADS``, each carrying its own precondition); the
    server refuses each with 412 instead of clobbering the successor's newer
    bytes.
    """

    headers: list[str] = []
    if if_match:
        headers.append(f"If-Match: {if_match}")
    elif if_none_match:
        headers.append("If-None-Match: *")
    return headers


def _raise_if_precondition_failed(exc: BaseException, rel_path: str) -> None:
    code = getattr(exc, "code", None)
    if code == 412:
        raise CloudSyncFenceLost(
            f"cloud write precondition failed for {rel_path} (412)"
        ) from exc


def _conditional_put(
    client: Any,
    rel_path: str,
    local_path: str,
    if_match: Optional[str],
    if_none_match: bool,
) -> Optional[str]:
    """PUT one file through webdav3 with preconditions; return the new ETag.

    ``Client.upload_sync`` has no header hook, so this mirrors its file
    branch on ``execute_request`` directly (same action, same URN quoting).
    """

    from webdav3.urn import Urn

    urn = Urn(rel_path)
    headers = _precondition_headers(if_match, if_none_match)
    try:
        with open(local_path, "rb") as handle:
            response = client.execute_request(
                action="upload",
                path=urn.quote(),
                data=handle,
                headers_ext=headers or None,
            )
    except Exception as exc:
        _raise_if_precondition_failed(exc, rel_path)
        raise
    try:
        etag = response.headers.get("ETag") or response.headers.get("OC-ETag")
    except Exception:
        etag = None
    return str(etag) if etag else None


def _conditional_delete(client: Any, rel_path: str, if_match: Optional[str]) -> None:
    from webdav3.urn import Urn

    urn = Urn(rel_path)
    headers = _precondition_headers(if_match, False)
    try:
        client.execute_request(
            action="clean", path=urn.quote(), headers_ext=headers or None
        )
    except Exception as exc:
        _raise_if_precondition_failed(exc, rel_path)
        raise


class NextcloudWorkspaceSync(WorkspaceSyncBase):
    """Nextcloud sync client — agent-service user + password, classic WebDAV."""

    def __init__(
        self,
        workspace_path: Path,
        *,
        webdav_url: str,
        webdav_user: str,
        webdav_password: str,
        poll_interval: int = 15,
        workspace_backend: Optional["WorkspaceBackend"] = None,
        mount_subdir: str = "",
    ) -> None:
        super().__init__(
            workspace_path,
            poll_interval=poll_interval,
            workspace_backend=workspace_backend,
            mount_subdir=mount_subdir,
        )
        self._webdav_url = webdav_url
        self._webdav_user = webdav_user
        self._webdav_password = webdav_password
        self._client = None
        # client.list(get_info=True) returns absolute server paths
        # (e.g. /remote.php/dav/files/user/folder/file.txt) — we strip
        # this prefix to normalize to paths relative to the session folder.
        self._webdav_base_path = urlparse(webdav_url).path.rstrip("/") + "/"
        # One-shot tree listing (Depth: infinity PROPFIND). Nextcloud's DAV
        # server always allows it (there is no setting); a server that
        # refuses it anyway is remembered, so the per-dir walk isn't preceded
        # by a doomed probe on every pull.
        self._http: Optional[Any] = None
        self._depth_infinity_unsupported = False

    def _get_client(self):
        if self._client is None:
            from webdav3.client import Client

            client = Client(
                {
                    "webdav_hostname": self._webdav_url.rstrip("/"),
                    "webdav_login": self._webdav_user,
                    "webdav_password": self._webdav_password,
                    # No HEAD existence probe before mkdir/info/list/download
                    # (a download was 6 requests, now 3). A missing resource
                    # is still a 404 -> RemoteResourceNotFound, which
                    # _marker_missing relies on; a missing mkdir parent is a
                    # 409 instead of RemoteParentNotFound, and
                    # _ensure_remote_dirs treats both alike.
                    "webdav_disable_check": True,
                }
            )
            # Echoing Nextcloud's one non-Secure cookie (``cookie_test``)
            # makes it mint a session token on every request; sending none
            # leaves each request at the cost of the login alone.
            client.session.cookies = NoCookieJar()
            self._client = client
        return self._client

    async def _ensure_ready(self) -> None:
        self._get_client()

    async def _ensure_remote_dir(
        self,
        rel_dir: str,
        *,
        before_write: Optional[Callable[[], Awaitable[None]]] = None,
    ) -> None:
        if before_write is not None:
            await before_write()
        client = self._get_client()
        # webdav3 answers True for 200/201 and for 405 (already exists),
        # raises for an error status, and returns False for anything else.
        # Only the first two may be cached as "exists".
        if not await asyncio.to_thread(client.mkdir, rel_dir):
            raise RuntimeError(f"MKCOL {rel_dir} did not confirm the directory")

    async def _upload_file(
        self,
        rel_path: str,
        local_path: str,
        *,
        before_write: Optional[Callable[[], Awaitable[None]]] = None,
        if_match: Optional[str] = None,
        if_none_match: bool = False,
    ) -> Optional[str]:
        if before_write is not None:
            await before_write()
        client = self._get_client()
        return await asyncio.to_thread(
            _conditional_put,
            client,
            rel_path,
            local_path,
            if_match,
            if_none_match,
        )

    async def _delete_remote_file(
        self,
        rel_path: str,
        *,
        before_write: Optional[Callable[[], Awaitable[None]]] = None,
        if_match: Optional[str] = None,
    ) -> None:
        if before_write is not None:
            await before_write()
        client = self._get_client()
        try:
            await asyncio.to_thread(_conditional_delete, client, rel_path, if_match)
        except Exception as exc:
            if not self._marker_missing(exc):
                raise

    async def _remote_etag(self, rel_path: str) -> Optional[str]:
        client = self._get_client()
        try:
            info = await asyncio.to_thread(client.info, rel_path)
        except Exception as exc:
            if self._marker_missing(exc):
                return None
            raise
        etag = (info or {}).get("etag") if isinstance(info, dict) else None
        return str(etag) if etag else None

    async def _list_remote_files(self, rel_dir: str = "") -> list[dict]:
        client = self._get_client()
        raw = await asyncio.to_thread(client.list, rel_dir or "/", get_info=True)
        return _normalize_dav_listing(raw, self._webdav_base_path)

    async def _list_remote_tree_fast(self) -> Optional[list[dict]]:
        """One ``Depth: infinity`` PROPFIND for the whole mount subtree.

        Measured on the k3d cluster (2026-08-08): the webdav3 per-directory
        walk costs ~2.5s/dir (its ``list()`` runs a ``check()`` PROPFIND
        before the real one, on a server where each PROPFIND is ~0.6s of
        PHP); the same tree comes back from a single infinity PROPFIND in
        ~1s. Nextcloud's DAV server enables Depth: infinity unconditionally
        (checked on 31). Only a refusal (``_refuses_depth_infinity``) flips
        the per-instance capability flag so the walk takes over for good;
        any other non-207 answer costs this pull's fast path only.
        """
        if self._depth_infinity_unsupported:
            return None
        import httpx

        if self._http is None:
            self._http = httpx.AsyncClient(
                auth=(self._webdav_user, self._webdav_password),
                timeout=httpx.Timeout(30.0, read=60.0),
                cookies=NoCookieJar(),
            )
        try:
            resp = await self._http.request(
                "PROPFIND",
                self._webdav_url.rstrip("/") + "/",
                headers={"Depth": "infinity", "Content-Type": "application/xml"},
                content=_TREE_PROPFIND_BODY,
            )
        except Exception as e:
            # Transport trouble is not a capability verdict — fall back for
            # THIS pull only (the walk sees the same network and reports the
            # real error under the caller's strict rules).
            logger.debug("Depth-infinity PROPFIND failed (%s); using walk", e)
            return None
        if resp.status_code != 207:
            if not _refuses_depth_infinity(resp.status_code, resp.text):
                # Auth, a missing folder, a busy or restarting server: the
                # walk handles this pull and the next pull asks again.
                logger.debug(
                    "Depth-infinity PROPFIND answered HTTP %d; walking this pull",
                    resp.status_code,
                )
                return None
            self._depth_infinity_unsupported = True
            logger.info(
                "Depth-infinity PROPFIND unsupported here (HTTP %d) — "
                "per-directory walk from now on",
                resp.status_code,
            )
            return None
        try:
            return self._parse_multistatus(resp.text)
        except Exception as e:
            logger.warning("Depth-infinity PROPFIND parse failed (%s); using walk", e)
            return None

    def _parse_multistatus(self, xml_text: str) -> list[dict]:
        """Shape a DAV multistatus into the sync algorithm's listing dicts.

        Only the 200-status propstat of each response carries live props
        (404 propstats enumerate the props a node lacks — e.g. no
        getcontentlength on collections). Hrefs are percent-encoded and
        server-absolute; both sides of the base-path strip are unquoted so
        encoding differences can't break the prefix match.
        """
        root = ET.fromstring(xml_text)
        base = unquote(self._webdav_base_path)
        out: list[dict] = []
        for resp in root.iter("{DAV:}response"):
            href_el = resp.find("{DAV:}href")
            if href_el is None or not href_el.text:
                continue
            href = unquote(href_el.text)
            prop = None
            for ps in resp.findall("{DAV:}propstat"):
                status_el = ps.find("{DAV:}status")
                if status_el is not None and " 200 " in f" {status_el.text or ''} ":
                    prop = ps.find("{DAV:}prop")
                    break
            if prop is None:
                continue
            rtype = prop.find("{DAV:}resourcetype")
            isdir = rtype is not None and rtype.find("{DAV:}collection") is not None
            etag_el = prop.find("{DAV:}getetag")
            etag = (etag_el.text or "") if etag_el is not None else ""
            size_el = prop.find("{DAV:}getcontentlength")
            try:
                size: Optional[int] = (
                    int(size_el.text)
                    if size_el is not None and size_el.text not in (None, "")
                    else None
                )
            except (TypeError, ValueError):
                size = None
            rel = href[len(base) :] if href.startswith(base) else href.strip("/")
            rel = rel.strip("/")
            if not rel:
                continue  # the mount collection itself
            out.append({"path": rel, "etag": etag, "isdir": isdir, "size": size})
        return out

    async def _download_file(self, rel_path: str, local_path: str) -> None:
        client = self._get_client()
        await asyncio.to_thread(
            client.download_sync,
            remote_path=rel_path,
            local_path=local_path,
        )

    async def aclose(self) -> None:
        # webdav3 client has no explicit close; drop the reference so GC reclaims.
        self._client = None
        if self._http is not None:
            try:
                await self._http.aclose()
            except Exception:
                pass
            self._http = None
