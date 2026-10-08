"""WebDAV tools.

Provides file access to WebDAV (Nextcloud, ownCloud, or any WebDAV server)
attached as a datasource. Read tools (list, read, info) are always available.
Write tools (write, delete) are only injected when the datasource is not
marked read-only.

Connection is established by the WebDAV connection factory (agent.connectors.connections)
and injected via ToolContext.get_datasource("webdav").
"""

import asyncio
import hashlib
import logging
import posixpath
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from langchain_core.tools import tool

from agent.tools.context import ToolContext

from shared.tool_catalog.definitions import (
    WEBDAV_TOOLS_METADATA as WEBDAV_TOOLS_METADATA,
)

logger = logging.getLogger(__name__)


def create_webdav_tools(context: ToolContext) -> List[Any]:
    """Create WebDAV tools with injected context.

    Args:
        context: ToolContext with a WebDAV connector (webdav3.client.Client)

    Returns:
        List of LangChain tool functions
    """
    client = context.get_datasource("webdav")
    if not client:
        raise ValueError("WebDAV connector not available in context")

    workspace = context.workspace_manager

    @tool
    def webdav_list(path: str = "/", recursive: bool = False) -> str:
        """List files and folders in WebDAV.

        Args:
            path: Directory path to list (default: root "/")
            recursive: If True, list all files recursively

        Returns:
            Formatted list of files with sizes and types
        """
        try:
            items = client.list(path, get_info=True)
            if not items:
                return f"No files found at {path}"

            lines = []
            for item in items:
                name = item.get("path", "").rstrip("/").split("/")[-1]
                if not name:
                    continue  # Skip the directory itself
                is_dir = item.get("isdir", False)
                size = item.get("size", "")
                type_icon = "[DIR]" if is_dir else "[FILE]"
                size_str = f"  {_human_size(int(size))}" if size and not is_dir else ""
                lines.append(f"  {type_icon} {name}{size_str}")

                if recursive and is_dir:
                    sub_path = path.rstrip("/") + "/" + name
                    try:
                        sub_items = client.list(sub_path, get_info=True)
                        for sub in sub_items:
                            sub_name = sub.get("path", "").rstrip("/").split("/")[-1]
                            if not sub_name:
                                continue
                            sub_is_dir = sub.get("isdir", False)
                            sub_size = sub.get("size", "")
                            sub_icon = "[DIR]" if sub_is_dir else "[FILE]"
                            sub_size_str = (
                                f"  {_human_size(int(sub_size))}"
                                if sub_size and not sub_is_dir
                                else ""
                            )
                            lines.append(
                                f"    {sub_icon} {name}/{sub_name}{sub_size_str}"
                            )
                    except Exception:
                        lines.append(f"    (could not list {name}/)")

            if not lines:
                return f"Empty directory: {path}"
            return f"WebDAV — {path}:\n" + "\n".join(lines)
        except Exception as e:
            return f"Error listing {path}: {e}"

    @tool
    async def webdav_read(path: str, target: str = "") -> str:
        """Download a file from WebDAV into the workspace.

        Args:
            path: File path in WebDAV (e.g. "/documents/report.pdf")
            target: Local filename in workspace (default: same as source filename)

        Returns:
            Confirmation with local path, or error message
        """
        try:
            filename = target or posixpath.basename(path.rstrip("/"))
            if not filename:
                return "Error: could not determine filename from path"

            workspace_path = posixpath.join("documents", filename)
            with tempfile.TemporaryDirectory(prefix="webdav_dl_") as temp_dir:
                local_path = Path(temp_dir) / Path(filename).name
                await asyncio.to_thread(
                    client.download_sync,
                    remote_path=path,
                    local_path=str(local_path),
                )
                data = await asyncio.to_thread(local_path.read_bytes)

                # Phase 3 (D7): stash a cloud snapshot-anchor for this file so a
                # later cite_* persists its drift fingerprint + live pointer onto
                # the source.
                try:
                    anchor = await asyncio.to_thread(
                        _build_cloud_anchor,
                        client,
                        path,
                        str(local_path),
                    )
                except Exception as e:
                    logger.debug("Could not build cloud anchor for %s: %s", path, e)
                    anchor = None

            # A backend path may be an object key or a path on another pod.
            # Only the operation-scoped staging path above is local. Keep the
            # backend write and its provenance update in one per-target
            # critical section so parallel reads cannot cross their ordering.
            async with context.cloud_anchor_write_lock(workspace_path):
                await asyncio.to_thread(
                    workspace.backend.write_file,
                    workspace_path,
                    data,
                )

                if anchor:
                    # Persistent sessions bind this seam to a durable
                    # per-thread upsert. Await it before reporting success so
                    # provenance cannot silently disappear at the next claim.
                    await context.persist_cloud_anchor(workspace_path, anchor)

            return f"Downloaded {path} → {workspace_path} ({_human_size(len(data))})"
        except Exception as e:
            return f"Error downloading {path}: {e}"

    @tool
    def webdav_info(path: str) -> str:
        """Get metadata about a file or folder in WebDAV.

        Args:
            path: File or folder path in WebDAV

        Returns:
            File metadata (size, modified date, content type)
        """
        try:
            info = client.info(path)
            if not info:
                return f"No info available for {path}"

            lines = [f"WebDAV — {path}:"]
            if "size" in info:
                lines.append(f"  Size: {_human_size(int(info['size']))}")
            if "modified" in info:
                lines.append(f"  Modified: {info['modified']}")
            if "content_type" in info:
                lines.append(f"  Type: {info['content_type']}")
            if "isdir" in info:
                lines.append(f"  Directory: {info['isdir']}")
            return "\n".join(lines)
        except Exception as e:
            return f"Error getting info for {path}: {e}"

    @tool
    def webdav_write(source: str, remote_path: str) -> str:
        """Upload a file from the workspace to WebDAV.

        Args:
            source: Relative path within workspace (e.g. "output/report.pdf")
            remote_path: Target path in WebDAV (e.g. "/project-files/report.pdf")

        Returns:
            Confirmation with file size, or error message
        """
        try:
            if not workspace.backend.is_file(source):
                return f"Error: file not found in workspace: {source}"

            # Create parent directories on WebDAV if needed
            parent = "/".join(remote_path.rstrip("/").split("/")[:-1])
            if parent and parent != "/":
                try:
                    client.mkdir(parent)
                except Exception:
                    pass  # Directory may already exist

            # The WebDAV client requires a local filename. Materialize through
            # the workspace abstraction instead of treating a backend path as
            # local agent storage.
            with workspace.local_copy(source) as local_path:
                client.upload_sync(
                    remote_path=remote_path,
                    local_path=str(local_path),
                )
                size = local_path.stat().st_size
            return f"Uploaded {source} → {remote_path} ({_human_size(size)})"
        except Exception as e:
            return f"Error uploading {source} to {remote_path}: {e}"

    @tool
    def webdav_delete(path: str) -> str:
        """Delete a file or folder from WebDAV.

        Args:
            path: Path in WebDAV to delete (e.g. "/project-files/old-report.pdf")

        Returns:
            Confirmation message, or error message
        """
        try:
            client.clean(path)
            return f"Deleted {path} from WebDAV"
        except Exception as e:
            return f"Error deleting {path}: {e}"

    return [webdav_list, webdav_read, webdav_info, webdav_write, webdav_delete]


def _human_size(size_bytes: int) -> str:
    """Format bytes as human-readable size."""
    for unit in ("B", "KB", "MB", "GB"):
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}" if unit != "B" else f"{size_bytes} B"
        size_bytes /= 1024
    return f"{size_bytes:.1f} TB"


def _webdav_base_url(client: Any) -> str:
    """Best-effort base URL (hostname) of a webdav3 client.

    webdav3 stows the connection options on ``client.webdav`` (a settings
    object with a ``hostname`` attribute). Guarded so an internal API change
    degrades to "no live pointer" rather than raising.
    """
    settings = getattr(client, "webdav", None)
    host = getattr(settings, "hostname", None) if settings is not None else None
    return str(host) if host else ""


def _file_sha256(local_path: str) -> Optional[str]:
    """SHA-256 of a file's raw bytes, streamed; None on any I/O error."""
    try:
        h = hashlib.sha256()
        with open(local_path, "rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                h.update(chunk)
        return h.hexdigest()
    except OSError as e:
        logger.debug("cloud-anchor hash failed for %s: %s", local_path, e)
        return None


def _build_cloud_anchor(
    client: Any, remote_path: str, local_path: str
) -> Dict[str, Any]:
    """Capture a Phase-3 (D7) cloud snapshot-anchor for a downloaded file.

    Best-effort and never raises: returns the drift fingerprint (``etag`` +
    raw-bytes ``file_sha256``) plus a best-effort live pointer (``backend``,
    ``path``, ``webdav_url``). A later ``cite_*`` persists this onto the
    source's ``metadata.cloud`` so the citation records what it actually cited
    and can be drift-checked / re-fetched on view.
    """
    anchor: Dict[str, Any] = {"backend": "webdav", "path": remote_path}

    base = _webdav_base_url(client)
    if base:
        anchor["webdav_url"] = base.rstrip("/") + "/" + remote_path.lstrip("/")

    # File metadata via PROPFIND (etag / modified / content_type / size).
    try:
        info = client.info(remote_path) or {}
        for key in ("etag", "modified", "created", "content_type", "size"):
            val = info.get(key)
            if val not in (None, ""):
                anchor[key] = val
    except Exception as e:  # webdav3 raises assorted client errors
        logger.debug("cloud-anchor info() failed for %s: %s", remote_path, e)

    file_hash = _file_sha256(local_path)
    if file_hash:
        anchor["file_sha256"] = file_hash

    anchor["captured_at"] = datetime.now(timezone.utc).isoformat()
    return anchor
