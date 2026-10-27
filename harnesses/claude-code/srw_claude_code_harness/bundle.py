"""Input staging, workspace archive, result bundle and upload (spec B3 steps 1, 6)."""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import posixpath
import shlex
import tarfile
from collections.abc import AsyncIterator, Sequence
from pathlib import Path

import httpx

from .task import InputFile, UploadTarget
from .workspace import Workspace, WorkspaceError

ARCHIVE_FILE_LIMIT_BYTES = 256 * 1024 * 1024
DOWNLOAD_CHUNK_BYTES = 1024 * 1024
_UPLOAD_CHUNK_BYTES = 1024 * 1024
_BACKOFF_SECONDS = (2.0, 4.0)
# Never archived: saved tool output, the env file (may hold passwords), shell scratch.
_EXCLUDED = ("./.srw/tool-output", "./.srw-env", "./.srw-run")


class BundleError(RuntimeError):
    pass


async def stage_inputs(
    workspace: Workspace, inputs: Sequence[InputFile], *, client: httpx.AsyncClient
) -> list[dict]:
    staged = []
    for item in inputs:
        target = workspace.resolve(posixpath.join(workspace.root, item.path))
        digest = hashlib.sha256()
        size = 0

        async def chunks(response: httpx.Response) -> AsyncIterator[bytes]:
            nonlocal size
            async for chunk in response.aiter_bytes(DOWNLOAD_CHUNK_BYTES):
                digest.update(chunk)
                size += len(chunk)
                yield chunk

        try:
            async with client.stream("GET", item.url) as response:
                if not response.is_success:
                    raise BundleError(f"input {item.path}: HTTP {response.status_code}")
                await workspace.write_stream(target, chunks(response))
        except BundleError:
            raise
        except (httpx.HTTPError, WorkspaceError) as exc:
            with contextlib.suppress(Exception):
                await workspace.run(f"rm -f -- {shlex.quote(target)}")
            raise BundleError(f"input {item.path}: download failed: {exc}") from exc
        staged.append({"path": item.path, "bytes": size, "sha256": digest.hexdigest()})
    return staged


def _excluded(path: str) -> bool:
    return any(path == e or path.startswith(e + "/") for e in _EXCLUDED)


async def archive_workspace(
    workspace: Workspace, dest: Path, *, limit_bytes: int = ARCHIVE_FILE_LIMIT_BYTES
) -> dict:
    limit = int(limit_bytes)
    excludes = " ".join(f"--exclude={shlex.quote(e)}" for e in _EXCLUDED)
    # Oversized files are listed by `find` and fed to tar as literal exclude patterns.
    # tar exits 1 when a file changed while it was read: that is still an archive.
    command = (
        f"find . -type f -size +{limit}c | "
        f"tar --anchored --no-wildcards --exclude-from=- {excludes} -czf - . ; "
        "rc=$?; [ $rc -le 1 ]"
    )
    written = 0
    try:
        with dest.open("wb") as out:
            async with contextlib.aclosing(workspace.read_stream(command)) as stream:
                async for chunk in stream:
                    out.write(chunk)
                    written += len(chunk)
    except WorkspaceError as exc:
        raise BundleError(f"workspace archive failed: {exc}") from exc
    listing = await workspace.run(
        f"cd -- {shlex.quote(workspace.root)} && find . -type f -size +{limit}c",
        capture_limit=4 * 1024 * 1024,
    )
    if listing.exit_code != 0:
        raise BundleError(f"listing oversized files failed: {listing.stderr.strip()}")
    skipped = [
        line for line in listing.stdout.splitlines() if line and not _excluded(line)
    ]
    return {"bytes": written, "skipped": sorted(skipped)}


def build_bundle(
    run_dir: Path, *, transcript: Path, result: Path, workspace_archive: Path | None
) -> Path:
    out = run_dir / "bundle.tar.gz"
    with tarfile.open(out, "w:gz") as tar:
        tar.add(transcript, arcname="transcript.jsonl")
        tar.add(result, arcname="result.json")
        if workspace_archive is not None:
            tar.add(workspace_archive, arcname="workspace.tar.gz")
    return out


async def _file_chunks(path: Path) -> AsyncIterator[bytes]:
    # Opened per call, so every retry starts from the beginning of the file.
    with path.open("rb") as handle:
        while chunk := handle.read(_UPLOAD_CHUNK_BYTES):
            yield chunk


async def upload(
    target: UploadTarget,
    path: Path,
    *,
    client: httpx.AsyncClient,
    attempts: int = 3,
) -> int:
    headers = dict(target.headers) | {"Content-Length": str(path.stat().st_size)}
    last = "no attempt made"
    for attempt in range(max(1, attempts)):
        if attempt:
            delays = _BACKOFF_SECONDS
            await asyncio.sleep(delays[min(attempt - 1, len(delays) - 1)])
        try:
            response = await client.request(
                target.method,
                target.url,
                content=_file_chunks(path),
                headers=headers,
            )
        except httpx.TransportError as exc:
            last = f"{type(exc).__name__}: {exc}"
            continue
        if response.status_code >= 500:
            last = f"HTTP {response.status_code}"
            continue
        if not response.is_success:
            raise BundleError(f"upload failed: HTTP {response.status_code}")
        return response.status_code
    raise BundleError(f"upload failed after {max(1, attempts)} attempts: {last}")
