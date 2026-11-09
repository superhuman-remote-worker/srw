"""Shared WebDAV PROPFIND multistatus parsing.

Both ``OpenCloudBackend`` and ``NextcloudBackend`` walk their project
folders with ``Depth: 1`` PROPFIND requests and parse the resulting
``<d:multistatus>`` body into ``ProjectFolderEntry`` rows. The two servers
are both sabre/dav-compatible (``xmlns:s="http://sabredav.org/ns"``,
``d:``-prefixed properties), so a single regex-based parser serves both.

Regex rather than a full XML parser: the PROPFIND output is structurally
regular enough that this is safe, and it keeps the cloud package free of an
XML-parser dependency. Each backend owns its own authenticated
``_propfind_depth_one`` (bearer vs. basic auth differ); only the pure
body→entries parsing lives here.
"""

from __future__ import annotations

import re
from urllib.parse import unquote, urlsplit

from orchestrator.services.cloud.handles import ProjectFolderEntry

# PROPFIND response patterns — module-level so they compile once.
_PROPFIND_RESPONSE_RE = re.compile(
    r"<d:response[^>]*>(.*?)</d:response>", re.DOTALL | re.IGNORECASE
)
_PROPFIND_HREF_RE = re.compile(r"<d:href[^>]*>([^<]+)</d:href>", re.IGNORECASE)
_PROPFIND_COLLECTION_RE = re.compile(r"<d:collection\s*/>", re.IGNORECASE)
_PROPFIND_CONTENTLENGTH_RE = re.compile(
    r"<d:getcontentlength[^>]*>(\d+)</d:getcontentlength>", re.IGNORECASE
)
_PROPFIND_ETAG_RE = re.compile(r"<d:getetag[^>]*>([^<]+)</d:getetag>", re.IGNORECASE)
_PROPFIND_CONTENTTYPE_RE = re.compile(
    r"<d:getcontenttype[^>]*>([^<]+)</d:getcontenttype>", re.IGNORECASE
)


def _href_path(raw: str) -> str:
    """An href's percent-decoded path; RFC 4918 allows a full URI too."""
    href = raw.strip()
    if "://" in href:
        href = urlsplit(href).path
    return unquote(href)


def first_href_path(xml: str) -> str | None:
    """The decoded path of the first ``<d:href>`` in a multistatus body (a
    ``Depth: 0`` PROPFIND's self entry), or ``None`` when there is none."""
    match = _PROPFIND_HREF_RE.search(xml or "")
    return _href_path(match.group(1)) if match else None


def hrefs_outside_prefix(xml: str, *, href_prefix: str) -> bool:
    """True when the body holds responses but none of their hrefs starts
    with ``href_prefix``. A correct server always answers with the walked
    collection's own entry, so this means the server writes another root
    into its hrefs than the caller expects: every entry would be dropped
    and the folder would read as empty."""
    prefix = unquote(href_prefix)
    seen = False
    for resp in _PROPFIND_RESPONSE_RE.finditer(xml or ""):
        href_match = _PROPFIND_HREF_RE.search(resp.group(1))
        if not href_match:
            continue
        seen = True
        if _href_path(href_match.group(1)).startswith(prefix):
            return False
    return seen


def parse_propfind_entries(
    xml: str, *, href_prefix: str, self_path: str = ""
) -> list[ProjectFolderEntry]:
    """Pull file + directory entries out of a PROPFIND multistatus body.

    ``href_prefix`` is the URL path prefix to strip so returned paths are
    relative to the folder root (no leading slash). Both ``href_prefix``
    and the ``<d:href>`` values are URL-decoded before comparison — servers
    may return a literal character in the href even when the request URL had
    it percent-encoded (e.g. OpenCloud's literal ``$``, Nextcloud's spaces),
    which would otherwise break a naive ``startswith`` check. Entries pointing
    at the walk target itself — the root (empty path after stripping) or the
    subdirectory named by ``self_path`` — are dropped, so a Depth:1 walk does
    not re-emit a directory it already surfaced as a child of its parent
    (the double-subdir bug, design §11.5).
    """
    decoded_prefix = unquote(href_prefix)
    entries: list[ProjectFolderEntry] = []
    for resp in _PROPFIND_RESPONSE_RE.finditer(xml):
        block = resp.group(1)
        href_match = _PROPFIND_HREF_RE.search(block)
        if not href_match:
            continue
        href = _href_path(href_match.group(1))
        if not href.startswith(decoded_prefix):
            continue
        rel_path = href[len(decoded_prefix) :].rstrip("/")
        if rel_path == self_path.strip("/"):
            # The walk target's own self-entry (the root, or the subdir this
            # Depth:1 PROPFIND is expanding). Its parent walk already emitted
            # it; re-emitting here is the double-subdir bug (design §11.5).
            continue
        is_dir = bool(_PROPFIND_COLLECTION_RE.search(block))
        size_match = _PROPFIND_CONTENTLENGTH_RE.search(block)
        etag_match = _PROPFIND_ETAG_RE.search(block)
        ctype_match = _PROPFIND_CONTENTTYPE_RE.search(block)
        entries.append(
            ProjectFolderEntry(
                path=rel_path,
                is_dir=is_dir,
                size=int(size_match.group(1)) if size_match else 0,
                etag=etag_match.group(1).strip().strip('"') if etag_match else "",
                content_type=ctype_match.group(1).strip() if ctype_match else "",
            )
        )
    return entries
