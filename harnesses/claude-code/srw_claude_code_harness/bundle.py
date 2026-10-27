"""Input staging, workspace archive, result bundle and upload (spec B3 steps 1, 6)."""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import posixpath
import shlex
import tarfile
import threading
from collections.abc import AsyncIterator, Sequence
from pathlib import Path

import httpx

from .task import InputFile, UploadTarget
from .workspace import Workspace, WorkspaceError, _failure

BUNDLE_NAME = "bundle.tar"
# The harness pod's ephemeral storage is a fixed 2Gi; peak disk use is about the
# workspace archive, a copy of it inside the bundle and the transcripts.
# A regular file over this size is skipped (listed in result.json) ...
ARCHIVE_FILE_LIMIT_BYTES = 256 * 1024 * 1024
# ... and the rest fill this many uncompressed bytes, smallest first.
ARCHIVE_TOTAL_LIMIT_BYTES = 768 * 1024 * 1024
# The archive file may exceed the selected bytes by tar headers and gzip framing;
# past the total plus this margin it is refused.
ARCHIVE_SIZE_MARGIN_BYTES = 64 * 1024 * 1024
# Bound on the workspace listing (type, size and path of every entry).
ARCHIVE_LISTING_LIMIT_BYTES = 64 * 1024 * 1024
# result.json lists at most this many omitted paths (the count is always there).
ARCHIVE_REPORT_LIMIT = 1000
# Time budgets: each input's download and transfer; the workspace archive.
STAGE_INPUT_TIMEOUT_SECONDS = 1800.0
ARCHIVE_TIMEOUT_SECONDS = 600.0
_ARCHIVE_GRACE_SECONDS = 5.0
DOWNLOAD_CHUNK_BYTES = 1024 * 1024
_UPLOAD_CHUNK_BYTES = 1024 * 1024
_BACKOFF_SECONDS = (2.0, 4.0)
# Never archived: saved tool output, the env file (may hold passwords), shell scratch.
_EXCLUDED = ("./.srw/tool-output", "./.srw-env", "./.srw-run")
_EXCLUDED_BYTES = tuple(e.encode() for e in _EXCLUDED)


class BundleError(RuntimeError):
    pass


async def stage_inputs(
    workspace: Workspace,
    inputs: Sequence[InputFile],
    *,
    client: httpx.AsyncClient,
    timeout: float = STAGE_INPUT_TIMEOUT_SECONDS,
) -> list[dict]:
    """Download each input into the workspace; each gets ``timeout`` seconds."""
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

        async def stage() -> None:
            async with client.stream("GET", item.url) as response:
                if not response.is_success:
                    raise BundleError(f"input {item.path}: HTTP {response.status_code}")
                await workspace.write_stream(target, chunks(response), timeout=timeout)

        try:
            await asyncio.wait_for(stage(), timeout + 5)
        except BundleError:
            raise
        except asyncio.TimeoutError as exc:
            with contextlib.suppress(Exception):
                await workspace.run(f"rm -f -- {shlex.quote(target)}")
            raise BundleError(
                f"input {item.path}: download timed out after {timeout:g} s"
            ) from exc
        except (httpx.HTTPError, WorkspaceError) as exc:
            with contextlib.suppress(Exception):
                await workspace.run(f"rm -f -- {shlex.quote(target)}")
            raise BundleError(f"input {item.path}: download failed: {exc}") from exc
        staged.append({"path": item.path, "bytes": size, "sha256": digest.hexdigest()})
    return staged


def _excluded(path: bytes) -> bool:
    return any(path == e or path.startswith(e + b"/") for e in _EXCLUDED_BYTES)


def _shown(paths: Sequence[bytes]) -> list[str]:
    return [p.decode("utf-8", errors="replace") for p in paths]


# Run in the root: prints "type TAB size TAB path NUL" for every entry but the
# excluded trees; find exits 1 after unreadable entries.
_LIST_COMMAND = (
    "find . -mindepth 1 \\( -path ./.srw/tool-output -o -path ./.srw-env "
    "-o -path ./.srw-run \\) -prune -o -printf '%y\\t%s\\t%p\\0'"
)
# Stores the NUL-separated member list sent on stdin; prints "size NUL path".
_SAVE_LIST_SCRIPT = (
    'mkdir -p -- "$HOME/.srw-run" && f=$(mktemp "$HOME/.srw-run/archive.XXXXXX") '
    '&& cat > "$f" && printf "%s\\0%s" "$(wc -c < "$f")" "$f"'
)


async def _list_entries(
    workspace: Workspace, timeout: float
) -> list[tuple[bytes, int, bytes]]:
    rc, stdout, stderr = await workspace._exec(
        f"cd -- {shlex.quote(workspace.root)} || exit 97\n{_LIST_COMMAND}",
        timeout=timeout,
        stdout_limit=ARCHIVE_LISTING_LIMIT_BYTES,
    )
    # GNU find exits 1 after unreadable entries but still lists the rest; a find
    # without -printf also exits 1, with nothing listed: that must not archive nothing.
    if rc not in (0, 1) or (rc == 1 and not stdout):
        raise BundleError(f"listing the workspace failed: {_failure(rc, stderr)}")
    entries = []
    for record in stdout.split(b"\0"):
        if not record:
            continue
        kind, _, rest = record.partition(b"\t")
        size, _, path = rest.partition(b"\t")
        try:
            entries.append((kind, int(size), path))
        except ValueError:
            raise BundleError("listing the workspace failed: bad entry") from None
    return entries


async def archive_workspace(
    workspace: Workspace,
    dest: Path,
    *,
    limit_bytes: int = ARCHIVE_FILE_LIMIT_BYTES,
    total_limit_bytes: int = ARCHIVE_TOTAL_LIMIT_BYTES,
    max_archive_bytes: int | None = None,
    exclude_paths: Sequence[str] = (),
    timeout: float = ARCHIVE_TIMEOUT_SECONDS,
) -> dict:
    """Write a gzipped tar of the workspace to ``dest``; returns its summary.

    Regular files over ``limit_bytes`` are skipped, ``exclude_paths`` (the staged
    inputs, relative to the root) are left out, and the rest fill
    ``total_limit_bytes`` smallest first (ties by path); files that do not fit
    are omitted. Directories, symlinks and other entries are archived as they
    are. tar gets the exact member list (NUL-separated, no recursion), so any
    file name round-trips. The whole step ends within ``timeout``; an archive
    file over ``max_archive_bytes`` (default: the total budget plus
    ``ARCHIVE_SIZE_MARGIN_BYTES``) fails it.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout

    def remaining() -> float:
        left = deadline - loop.time()
        if left <= 0:
            raise asyncio.TimeoutError
        return left

    try:
        return await asyncio.wait_for(
            _archive(
                workspace,
                dest,
                limit=int(limit_bytes),
                total_limit=int(total_limit_bytes),
                max_bytes=(
                    int(max_archive_bytes)
                    if max_archive_bytes is not None
                    else int(total_limit_bytes) + ARCHIVE_SIZE_MARGIN_BYTES
                ),
                inputs={
                    b"./" + posixpath.normpath(path).encode() for path in exclude_paths
                },
                remaining=remaining,
            ),
            timeout + _ARCHIVE_GRACE_SECONDS,
        )
    except asyncio.TimeoutError:
        dest.unlink(missing_ok=True)
        raise BundleError(
            f"workspace archive failed: timed out after {timeout:g} s"
        ) from None


async def _archive(
    workspace: Workspace,
    dest: Path,
    *,
    limit: int,
    total_limit: int,
    max_bytes: int,
    inputs: set[bytes],
    remaining,
) -> dict:
    try:
        entries = await _list_entries(workspace, remaining())
    except WorkspaceError as exc:
        raise BundleError(f"listing the workspace failed: {exc}") from exc
    files: list[tuple[int, bytes]] = []
    skipped: list[bytes] = []
    excluded_inputs: list[bytes] = []
    for kind, size, path in entries:
        if _excluded(path) or kind != b"f":
            continue
        if path in inputs:
            excluded_inputs.append(path)
        elif size > limit:
            skipped.append(path)
        else:
            files.append((size, path))
    files.sort()
    chosen: set[bytes] = set()
    omitted: list[tuple[int, bytes]] = []
    total = 0
    for size, path in files:
        if total + size <= total_limit:
            total += size
            chosen.add(path)
        else:
            omitted.append((size, path))
    members = [
        path
        for kind, _, path in entries
        if not _excluded(path) and (kind != b"f" or path in chosen)
    ]
    member_list = b"".join(path + b"\0" for path in members)
    try:
        saved = await workspace._checked(
            _SAVE_LIST_SCRIPT, member_list, timeout=remaining()
        )
    except WorkspaceError as exc:
        raise BundleError(f"workspace archive failed: {exc}") from exc
    size_text, _, list_file = saved.partition(b"\0")
    quoted = shlex.quote(list_file.decode("utf-8", errors="surrogateescape"))
    if (
        not list_file.startswith(b"/")
        or size_text.strip() != str(len(member_list)).encode()
    ):
        with contextlib.suppress(Exception):
            await workspace._exec(f"rm -f -- {quoted}", timeout=10)
        raise BundleError("workspace archive failed: the member list did not arrive")
    # tar exits 1 when a file changed while it was read: that is still an archive.
    # --ignore-failed-read turns an unreadable or vanished file into a warning.
    command = (
        f"list={quoted}\n"
        """trap 'rm -f -- "$list"' EXIT\n"""
        'tar --null --no-recursion --ignore-failed-read -czf - -T "$list"\n'
        "rc=$?; [ $rc -le 1 ]"
    )
    written = 0
    try:
        try:
            with dest.open("wb") as out:
                async with contextlib.aclosing(
                    workspace.read_stream(command, timeout=remaining(), limit=max_bytes)
                ) as stream:
                    async for chunk in stream:
                        out.write(chunk)
                        written += len(chunk)
        except WorkspaceError as exc:
            raise BundleError(f"workspace archive failed: {exc}") from exc
        with dest.open("rb") as check:
            magic = check.read(2)
        if magic != b"\x1f\x8b":
            raise BundleError(
                "workspace archive failed: tar produced no gzip output "
                "(is GNU tar installed in the workspace?)"
            )
    except BaseException:
        dest.unlink(missing_ok=True)
        raise
    omitted_paths = sorted(path for _, path in omitted)
    return {
        "bytes": written,
        "skipped": sorted(_shown(skipped)),
        "excludedInputs": sorted(_shown(excluded_inputs)),
        "omitted": _shown(omitted_paths[:ARCHIVE_REPORT_LIMIT]),
        "omittedCount": len(omitted),
        "omittedBytes": sum(size for size, _ in omitted),
        "truncated": bool(omitted),
    }


class _Abortable:
    """A file reader that raises once ``cancel`` is set (the bundle thread)."""

    def __init__(self, handle, cancel: threading.Event | None) -> None:
        self._handle = handle
        self._cancel = cancel

    def read(self, size: int = -1) -> bytes:
        if self._cancel is not None and self._cancel.is_set():
            raise BundleError("building the bundle was cancelled")
        return self._handle.read(size)


def build_bundle(
    run_dir: Path,
    *,
    transcript: Path,
    result: Path,
    workspace_archive: Path | None,
    cancel: threading.Event | None = None,
) -> Path:
    """Write the uncompressed ``bundle.tar`` (the archive is already gzipped).

    The workspace archive file is deleted as soon as it has been added. Blocking:
    call it with ``asyncio.to_thread``; setting ``cancel`` ends it early.
    """
    out = run_dir / BUNDLE_NAME
    with tarfile.open(out, "w") as tar:
        for path, name in (
            (transcript, "transcript.jsonl"),
            (result, "result.json"),
            (workspace_archive, "workspace.tar.gz"),
        ):
            if path is None:
                continue
            info = tar.gettarinfo(str(path), arcname=name)
            with path.open("rb") as handle:
                tar.addfile(info, _Abortable(handle, cancel))
            if path == workspace_archive:
                path.unlink()
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
