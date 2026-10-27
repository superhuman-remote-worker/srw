"""Entrypoint: ``python3 -m srw_claude_code_harness`` (spec B3, B5).

``run`` performs one harness run: read and validate the delivered files, pick the
credential, collect the ``workspaceEnv`` values, connect to the workspace, write
its environment file, stage the inputs, run the Claude Code session, write
``result.json`` and close the workspace. When the task names an upload target it
also archives the workspace, builds the bundle (an uncompressed ``bundle.tar``)
and uploads it; without one nothing is archived or uploaded. It never raises (an
unexpected exception becomes ``EXIT_HARNESS_ERROR``); every path writes
``result.json``, and the bundle is uploaded on every path once the target is
known.

Exit codes and their precedence:

- ``EXIT_CONFIG_ERROR`` (2) and ``EXIT_HARNESS_ERROR`` (1) before the session:
  invalid config/task/bindings, no credential, a ``workspaceEnv`` name missing
  from the environment, or input staging failed (2); SSH failure or an
  unexpected exception (1).
- After the session: ``EXIT_INTERRUPTED`` (4) when the wall-clock deadline or
  the stop request (SIGTERM) ended it, whatever the result says; else
  ``EXIT_RUN_ERROR`` (3) when there is no result, it is an error, or its
  subtype is not ``success``; else ``EXIT_OK`` (0).
- ``EXIT_OK`` alone is then replaced by a later failure: the workspace archive
  (1), a stop request while archiving or uploading (4), the upload (5).

Stop (SIGTERM / SRW cancel; the pod has ~30 s before SIGKILL): ``main`` sets the
``stop`` event and never cancels the task. Pending stages are cancelled, the
session is interrupted by ``run_session`` within its stop budget, the workspace
is not archived, and the bundle (transcript and result only) is uploaded within
``STOP_UPLOAD_SECONDS``.

Progress lines go to stdout as ``[srw-harness] <stage>``. Nothing printed or
written to ``result.json`` contains the credential, ``workspaceEnv`` values or a
URL (presigned URLs are credentials).
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import json
import logging
import os
import re
import signal
import sys
import tempfile
import threading
from collections.abc import Awaitable, Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx

from . import __version__ as HARNESS_VERSION
from .bindings import parse_bindings
from .bundle import (
    ARCHIVE_TIMEOUT_SECONDS,
    BUNDLE_NAME,
    BundleError,
    archive_workspace,
    build_bundle,
    stage_inputs,
    upload,
)
from .config import (
    ConfigError,
    HarnessConfig,
    config_hash,
    effective_settings,
    parse_config,
)
from .result import write_result
from .session import (
    SessionOutcome,
    build_options,
    cli_env,
    run_session,
    scrub_process_environment,
)
from .task import Task, parse_task
from .tools import make_tools
from .transcript import Transcript
from .workspace import Workspace, WorkspaceError

EXIT_OK = 0
EXIT_HARNESS_ERROR = 1
EXIT_CONFIG_ERROR = 2
EXIT_RUN_ERROR = 3
EXIT_INTERRUPTED = 4
EXIT_UPLOAD_ERROR = 5

# In order of preference.
CREDENTIAL_ENV = ("CLAUDE_CODE_OAUTH_TOKEN", "ANTHROPIC_API_KEY")
DELIVERY_ENV = ("SRW_CONFIG_FILE", "SRW_TASK_FILE", "SRW_BINDINGS_FILE")

# Once stop has fired, the whole bundle upload (retries included) gets this long.
STOP_UPLOAD_SECONDS = 10.0
# How long a stage cancelled by the stop request gets to unwind before it is
# abandoned.
CANCEL_WAIT_SECONDS = 2.0
CLOSE_WAIT_SECONDS = 5.0
HTTP_TIMEOUT = httpx.Timeout(60.0, connect=30.0)

_URL = re.compile(r"https?://\S+")
_MIN_SECRET_LENGTH = 4
_REASON_LIMIT = 1000


class _Failure(Exception):
    def __init__(self, code: int, reason: str) -> None:
        super().__init__(reason)
        self.code = code
        self.reason = reason


class _Stopped(Exception):
    """The stop request fired while a stage was running; the stage was cancelled."""


def _progress(message: str) -> None:
    print(f"[srw-harness] {message}", flush=True)


def _error(message: str) -> None:
    print(f"[srw-harness] {message}", file=sys.stderr, flush=True)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _describe(exc: BaseException) -> str:
    """The harness's own errors as their message, anything else with its type."""
    text = str(exc)
    if isinstance(exc, (BundleError, ConfigError, WorkspaceError)) and text:
        return text
    return f"{type(exc).__name__}: {text}" if text else type(exc).__name__


def _versions() -> dict[str, str | None]:
    versions: dict[str, str | None] = {
        "harness": HARNESS_VERSION,
        "sdk": None,
        "cli": None,
    }
    with contextlib.suppress(Exception):
        import claude_agent_sdk

        versions["sdk"] = getattr(claude_agent_sdk, "__version__", None)
    with contextlib.suppress(Exception):
        from claude_agent_sdk._cli_version import __cli_version__

        versions["cli"] = __cli_version__
    return versions


def _http_clients(
    transport: httpx.AsyncBaseTransport | None = None,
) -> tuple[httpx.AsyncClient, httpx.AsyncClient]:
    """The download client (follows redirects: dataset hosts such as GitHub and
    Hugging Face redirect to their storage) and the upload client (does not: a
    presigned URL is answered directly, never by a redirect)."""
    download = httpx.AsyncClient(
        follow_redirects=True, timeout=HTTP_TIMEOUT, transport=transport
    )
    upload_client = httpx.AsyncClient(
        follow_redirects=False, timeout=HTTP_TIMEOUT, transport=transport
    )
    return download, upload_client


def _read_json(path: Path, what: str) -> object:
    try:
        text = Path(path).read_text(encoding="utf-8")
    except OSError as exc:
        detail = exc.strerror or type(exc).__name__
        raise _Failure(
            EXIT_CONFIG_ERROR, f"cannot read the {what} file: {detail}"
        ) from exc
    except UnicodeDecodeError as exc:
        raise _Failure(EXIT_CONFIG_ERROR, f"the {what} file is not UTF-8") from exc
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise _Failure(
            EXIT_CONFIG_ERROR,
            f"the {what} file is not valid JSON "
            f"({exc.msg} at line {exc.lineno} column {exc.colno})",
        ) from exc


def _local_cwd(root: str, run_dir: Path) -> Path:
    """The CLI's local working directory.

    Claude Code tells the model its working directory and the tools resolve
    paths against the workspace root, so the CLI runs in a local directory with
    the root's own path when the harness can have one (the image provides it);
    otherwise in a per-run directory.
    """
    path = Path(root)
    with contextlib.suppress(OSError):
        path.mkdir(parents=True, exist_ok=True)
    if path.is_dir() and os.access(path, os.R_OK | os.X_OK):
        return path
    fallback = run_dir / "cwd"
    fallback.mkdir(mode=0o700, parents=True, exist_ok=True)
    return fallback


def _session_exit(outcome: SessionOutcome, timeout_seconds: float) -> tuple[int, str]:
    if outcome.interrupted:
        return EXIT_INTERRUPTED, "interrupted: stop requested"
    if outcome.timed_out:
        return (
            EXIT_INTERRUPTED,
            f"timed out: the {timeout_seconds:g} s wall-clock limit was reached",
        )
    result = outcome.result
    if result is None:
        detail = outcome.error or "the session ended without a result"
        return EXIT_RUN_ERROR, f"run error: no result ({detail})"
    subtype = getattr(result, "subtype", None)
    if subtype != "success":
        return EXIT_RUN_ERROR, f"run error: {subtype}"
    if getattr(result, "is_error", False):
        return EXIT_RUN_ERROR, "run error: the result is marked as an error"
    return EXIT_OK, "success"


def _write_partial_result(
    path: Path,
    *,
    config_data: object,
    config: HarnessConfig | None,
    outcome: SessionOutcome | None,
    started: datetime,
    finished: datetime,
    exit_code: int,
    exit_reason: str,
    versions: dict,
) -> None:
    """``result.json`` for a run that failed before the config and task were
    both parsed; same keys as :func:`write_result`."""
    data = {
        "version": 1,
        "exitCode": exit_code,
        "exitReason": exit_reason,
        "startedAt": started.isoformat(),
        "finishedAt": finished.isoformat(),
        "durationSeconds": (finished - started).total_seconds(),
        "config": effective_settings(config) if config is not None else None,
        "configHash": config_hash(config_data) if config_data is not None else None,
        "prompt": None,
        "inputs": [],
        "archive": None,
        "versions": dict(versions),
        "result": None,
        "timedOut": bool(getattr(outcome, "timed_out", False)),
        "interrupted": bool(getattr(outcome, "interrupted", False)),
        "error": None,
    }
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")
    os.replace(temporary, path)


class _Run:
    def __init__(
        self,
        *,
        config_file: Path,
        task_file: Path,
        bindings_file: Path,
        environ: Mapping[str, str],
        run_dir: Path,
        stop: asyncio.Event,
        client_factory: Any,
        http_client: httpx.AsyncClient | None,
        connect: Any,
    ) -> None:
        self.config_file = Path(config_file)
        self.task_file = Path(task_file)
        self.bindings_file = Path(bindings_file)
        self.environ = environ
        self.run_dir = Path(run_dir)
        self.stop = stop
        self.client_factory = client_factory
        self.http_client = http_client
        self.connect = connect
        self.result_path = self.run_dir / "result.json"
        self.transcript_path = self.run_dir / "transcript.jsonl"
        self.started = _now()
        self.versions = _versions()
        self.code = EXIT_HARNESS_ERROR
        self.reason = "harness error: the run did not finish"
        self.secrets: set[str] = set()
        self.config_data: object = None
        self.config: HarnessConfig | None = None
        self.task: Task | None = None
        self.transcript: Transcript | None = None
        self.workspace: Workspace | None = None
        self.outcome: SessionOutcome | None = None
        self.session_started = False
        self.inputs: list[dict] = []
        self.archive: dict | None = None
        self.archive_file: Path | None = None
        self._clients: tuple[httpx.AsyncClient, httpx.AsyncClient] | None = None
        self._own_clients = False

    # -- bookkeeping ---------------------------------------------------------

    def _set(self, code: int, reason: str) -> None:
        self.code = code
        self.reason = reason

    def _remember(self, value: str | None) -> None:
        if value and len(value) >= _MIN_SECRET_LENGTH:
            self.secrets.add(value)

    def _redact(self, text: str) -> str:
        for secret in sorted(self.secrets, key=len, reverse=True):
            text = text.replace(secret, "***")
        return _URL.sub("<url>", text)[:_REASON_LIMIT]

    def _note(self, payload: dict) -> None:
        if self.transcript is not None:
            with contextlib.suppress(Exception):
                self.transcript.record("harness", payload)

    def _close_transcript(self) -> None:
        if self.transcript is not None:
            with contextlib.suppress(Exception):
                self.transcript.close()

    def _http(self) -> tuple[httpx.AsyncClient, httpx.AsyncClient]:
        if self._clients is None:
            if self.http_client is not None:
                self._clients = (self.http_client, self.http_client)
            else:
                self._clients = _http_clients()
                self._own_clients = True
        return self._clients

    def _write_result(self) -> None:
        reason = self._redact(self.reason)
        outcome = self.outcome
        if outcome is not None and outcome.error:
            outcome = dataclasses.replace(outcome, error=self._redact(outcome.error))
        if self.config is None or self.task is None:
            _write_partial_result(
                self.result_path,
                config_data=self.config_data,
                config=self.config,
                outcome=outcome,
                started=self.started,
                finished=_now(),
                exit_code=self.code,
                exit_reason=reason,
                versions=self.versions,
            )
            return
        write_result(
            self.result_path,
            config_data=self.config_data,
            config=self.config,
            task=self.task,
            outcome=outcome,
            started=self.started,
            finished=_now(),
            exit_code=self.code,
            exit_reason=reason,
            inputs=self.inputs,
            archive=self.archive,
            versions=self.versions,
        )

    async def _unless_stopped(self, awaitable: Awaitable[Any]) -> Any:
        """Await ``awaitable`` unless the stop request fires first; then cancel it
        (waiting at most ``CANCEL_WAIT_SECONDS`` for it to unwind) and raise
        :class:`_Stopped`."""
        if self.stop.is_set():
            if asyncio.iscoroutine(awaitable):
                awaitable.close()
            raise _Stopped
        task = asyncio.ensure_future(awaitable)
        waiter = asyncio.ensure_future(self.stop.wait())
        try:
            await asyncio.wait({task, waiter}, return_when=asyncio.FIRST_COMPLETED)
        except asyncio.CancelledError:
            task.cancel()
            raise
        finally:
            waiter.cancel()
        if task.done():
            if task.cancelled():
                raise RuntimeError("a harness stage was cancelled")
            return task.result()
        task.cancel()
        await asyncio.wait({task}, timeout=CANCEL_WAIT_SECONDS)
        if task.done() and not task.cancelled():
            task.exception()  # retrieved: the stage was abandoned for the stop
        raise _Stopped

    # -- stages --------------------------------------------------------------

    async def execute(self) -> int:
        try:
            try:
                self.run_dir.mkdir(parents=True, exist_ok=True)
                self.transcript = Transcript(self.transcript_path)
                options = await self._prepare()
                await self._session(options)
            except _Failure as failure:
                self._set(failure.code, failure.reason)
            except _Stopped:
                self.outcome = SessionOutcome(
                    result=None, timed_out=False, interrupted=True, error=None
                )
                self._note({"event": "stop", "phase": "prepare"})
                self._set(
                    EXIT_INTERRUPTED, "interrupted: stop requested before the session"
                )
            except Exception as exc:
                self._set(EXIT_HARNESS_ERROR, f"harness error: {_describe(exc)}")
            try:
                await self._finish()
            except Exception as exc:
                if self.code == EXIT_OK:
                    self._set(EXIT_HARNESS_ERROR, f"harness error: {_describe(exc)}")
                _error(f"finishing the run failed: {self._redact(_describe(exc))}")
                with contextlib.suppress(Exception):
                    self._write_result()
        finally:
            await self._cleanup()
        reason = self._redact(self.reason)
        (_progress if self.code == EXIT_OK else _error)(f"exit {self.code}: {reason}")
        return self.code

    async def _prepare(self) -> Any:
        _progress("reading the delivered files")
        self.config_data = _read_json(self.config_file, "config")
        self.config = self._parse(parse_config, self.config_data, "config")
        self.task = self._parse(parse_task, _read_json(self.task_file, "task"), "task")
        for item in self.task.inputs:
            self._remember(item.url)
        if self.task.upload is not None:
            self._remember(self.task.upload.url)
            for _, value in self.task.upload.headers:
                self._remember(value)
        access = self._parse(
            parse_bindings, _read_json(self.bindings_file, "bindings"), "bindings"
        )
        credential = self._credential()
        workspace_env = self._workspace_env()

        _progress("connecting to the workspace")
        try:
            self.workspace = await self._unless_stopped(self.connect(access))
        except WorkspaceError as exc:
            raise _Failure(EXIT_HARNESS_ERROR, f"workspace error: {exc}") from exc
        _progress("writing the workspace environment file")
        try:
            await self._unless_stopped(self.workspace.write_env_file(workspace_env))
        except WorkspaceError as exc:
            raise _Failure(
                EXIT_HARNESS_ERROR,
                f"writing the workspace environment file failed: {exc}",
            ) from exc
        if self.task.inputs:
            _progress(f"staging {len(self.task.inputs)} input file(s)")
            download, _ = self._http()
            try:
                self.inputs = await self._unless_stopped(
                    stage_inputs(self.workspace, self.task.inputs, client=download)
                )
            except (BundleError, WorkspaceError, httpx.InvalidURL) as exc:
                raise _Failure(
                    EXIT_CONFIG_ERROR, f"input staging failed: {_describe(exc)}"
                ) from exc

        tools = make_tools(self.workspace, self.config.tools)
        env = cli_env(
            self.config,
            credential,
            run_dir=self.run_dir,
            path=self.environ.get("PATH") or os.defpath,
        )
        cwd = _local_cwd(self.workspace.root, self.run_dir)
        return build_options(self.config, tools=tools, env=env, cwd=cwd)

    @staticmethod
    def _parse(parser: Any, data: object, what: str) -> Any:
        try:
            return parser(data)
        except ConfigError as exc:
            raise _Failure(EXIT_CONFIG_ERROR, f"invalid {what}: {exc}") from exc

    def _credential(self) -> tuple[str, str]:
        for name in CREDENTIAL_ENV:
            value = (self.environ.get(name) or "").strip()
            if value:
                self._remember(value)
                return name, value
        raise _Failure(
            EXIT_CONFIG_ERROR,
            "no model credential: set CLAUDE_CODE_OAUTH_TOKEN or ANTHROPIC_API_KEY",
        )

    def _workspace_env(self) -> dict[str, str]:
        assert self.config is not None
        missing = [n for n in self.config.workspace_env if n not in self.environ]
        if missing:
            raise _Failure(
                EXIT_CONFIG_ERROR,
                "workspaceEnv names variables that are not set in the harness "
                f"environment: {', '.join(missing)}",
            )
        values = {name: self.environ[name] for name in self.config.workspace_env}
        for value in values.values():
            self._remember(value)
        return values

    async def _session(self, options: Any) -> None:
        assert self.config is not None and self.task is not None
        _progress(f"starting Claude Code (model {self.config.model})")
        self.session_started = True
        self.outcome = await run_session(
            self.task.prompt,
            options,
            transcript=self.transcript,
            deadline_seconds=self.config.limits.timeout_seconds,
            stop=self.stop,
            client_factory=self.client_factory,
        )
        code, reason = _session_exit(self.outcome, self.config.limits.timeout_seconds)
        self._set(code, reason)
        _progress(f"session ended: {self._redact(reason)}")

    def _stopped_after_session(self, stage: str) -> None:
        _progress(f"stop requested while {stage}")
        self._note({"event": "stop", "phase": stage})
        if self.code == EXIT_OK:
            self._set(EXIT_INTERRUPTED, f"interrupted: stop requested while {stage}")

    async def _finish(self) -> None:
        if self.stop.is_set() and self.code == EXIT_OK:
            self._stopped_after_session("finishing the run")
        _progress("writing result.json")
        self._write_result()
        # The archive only travels in the bundle: no target, no archive.
        has_target = self.task is not None and self.task.upload is not None
        if (
            has_target
            and self.workspace is not None
            and self.session_started
            and not self.stop.is_set()
        ):
            await self._archive()
            if self.stop.is_set() and self.code == EXIT_OK:
                # The stop fired as the archive completed: still interrupted.
                self._stopped_after_session("archiving the workspace")
            self._write_result()
        if has_target:
            await self._upload()
        self._write_result()

    async def _archive(self) -> None:
        assert self.workspace is not None
        _progress("archiving the workspace")
        dest = self.run_dir / "workspace.tar.gz"
        try:
            self.archive = await self._unless_stopped(
                archive_workspace(
                    self.workspace,
                    dest,
                    # Their checksums are in result.json already.
                    exclude_paths=[item["path"] for item in self.inputs],
                    timeout=ARCHIVE_TIMEOUT_SECONDS,
                )
            )
        except _Stopped:
            self._stopped_after_session("archiving the workspace")
            return
        except Exception as exc:
            error = self._redact(_describe(exc))
            self.archive = {"error": error}
            _error(f"workspace archive failed: {error}")
            if self.code == EXIT_OK:
                self._set(EXIT_HARNESS_ERROR, f"harness error: {error}")
            return
        self.archive_file = dest
        _progress(
            f"workspace archived ({self.archive['bytes']} bytes, "
            f"{len(self.archive['skipped'])} file(s) over the size limit skipped, "
            f"{self.archive['omittedCount']} omitted over the total budget)"
        )

    def _bundle(self, workspace_archive: Path | None, cancel: threading.Event) -> Path:
        """Blocking (run it in a thread): a fresh ``bundle.tar``; the workspace
        archive file is deleted once it is in the bundle."""
        # A fresh file: an abandoned upload may still hold the previous one open.
        (self.run_dir / BUNDLE_NAME).unlink(missing_ok=True)
        return build_bundle(
            self.run_dir,
            transcript=self.transcript_path,
            result=self.result_path,
            workspace_archive=workspace_archive,
            cancel=cancel,
        )

    def _upload_failed(self, reason: str) -> None:
        reason = self._redact(reason)
        _error(reason)
        if self.code == EXIT_OK:
            self._set(EXIT_UPLOAD_ERROR, reason)

    async def _upload(self) -> None:
        assert self.task is not None and self.task.upload is not None
        target = self.task.upload
        _, client = self._http()
        self._close_transcript()
        if not self.stop.is_set():
            cancel = threading.Event()
            archive, self.archive_file = self.archive_file, None
            try:
                bundle = await self._unless_stopped(
                    asyncio.to_thread(self._bundle, archive, cancel)
                )
                _progress(f"uploading the bundle ({bundle.stat().st_size} bytes)")
                status = await self._unless_stopped(
                    upload(target, bundle, client=client)
                )
            except _Stopped:
                cancel.set()  # an abandoned bundle thread stops at its next read
                self._stopped_after_session("uploading the bundle")
                self._write_result()
            except Exception as exc:
                self._upload_failed(_describe(exc))
                return
            else:
                _progress(f"bundle uploaded (HTTP {status})")
                return
            finally:
                if archive is not None:
                    archive.unlink(missing_ok=True)
        # Stop path: transcript and result only, within the remaining grace.
        cancel = threading.Event()

        async def partial() -> int:
            bundle = await asyncio.to_thread(self._bundle, None, cancel)
            _progress(f"uploading the partial bundle ({bundle.stat().st_size} bytes)")
            return await upload(target, bundle, client=client)

        try:
            status = await asyncio.wait_for(partial(), STOP_UPLOAD_SECONDS)
        except asyncio.TimeoutError:
            cancel.set()
            self._upload_failed(
                f"upload failed: no answer within {STOP_UPLOAD_SECONDS:g} s"
            )
        except Exception as exc:
            self._upload_failed(_describe(exc))
        else:
            _progress(f"partial bundle uploaded (HTTP {status})")

    async def _cleanup(self) -> None:
        self._close_transcript()
        if self.workspace is not None:
            with contextlib.suppress(Exception):
                await asyncio.wait_for(self.workspace.close(), CLOSE_WAIT_SECONDS)
        if self._own_clients and self._clients is not None:
            for client in self._clients:
                with contextlib.suppress(Exception):
                    await client.aclose()


async def run(
    *,
    config_file: Path,
    task_file: Path,
    bindings_file: Path,
    environ: Mapping[str, str],
    run_dir: Path,
    stop: asyncio.Event,
    client_factory: Any = None,
    http_client: httpx.AsyncClient | None = None,
    connect: Any = Workspace.connect,
) -> int:
    """One harness run; returns the exit code (see the module docstring).

    ``environ`` is the pod environment captured before the scrub; ``os.environ``
    is never read or changed here. ``client_factory`` replaces
    ``ClaudeSDKClient``, ``http_client`` serves both downloads and the upload,
    and ``connect`` replaces :meth:`Workspace.connect` (tests). Never raises
    except ``asyncio.CancelledError``.
    """
    return await _Run(
        config_file=config_file,
        task_file=task_file,
        bindings_file=bindings_file,
        environ=environ,
        run_dir=run_dir,
        stop=stop,
        client_factory=client_factory,
        http_client=http_client,
        connect=connect,
    ).execute()


async def _serve(paths: dict[str, Path], environ: dict[str, str], run_dir: Path) -> int:
    loop = asyncio.get_running_loop()
    stop = asyncio.Event()

    def request_stop(sig: signal.Signals) -> None:
        if not stop.is_set():
            _progress(f"{sig.name} received: stopping")
            stop.set()

    installed = []
    for sig in (signal.SIGTERM, signal.SIGINT):
        with contextlib.suppress(NotImplementedError, RuntimeError, ValueError):
            loop.add_signal_handler(sig, request_stop, sig)
            installed.append(sig)
    try:
        return await run(
            config_file=paths["SRW_CONFIG_FILE"],
            task_file=paths["SRW_TASK_FILE"],
            bindings_file=paths["SRW_BINDINGS_FILE"],
            environ=environ,
            run_dir=run_dir,
            stop=stop,
        )
    finally:
        for sig in installed:
            loop.remove_signal_handler(sig)


def main(argv: list[str] | None = None) -> int:
    """Run the harness from SRW's delivered files; returns the exit code.

    Takes no arguments (``argv`` is accepted and ignored). SIGTERM and SIGINT set
    the run's ``stop`` event; the run is never cancelled. A session close the run
    had to abandon is cancelled when the event loop shuts down; the CLI process
    then ends with the pod.
    """
    del argv
    paths: dict[str, Path] = {}
    for name in DELIVERY_ENV:
        value = os.environ.get(name)
        if not value:
            if name == "SRW_BINDINGS_FILE":
                _error(
                    "SRW_BINDINGS_FILE is not set: a workspace is required "
                    "(attach a workspace to the Job)"
                )
            else:
                _error(f"{name} is not set")
            return EXIT_CONFIG_ERROR
        if not Path(value).is_file():
            _error(f"Invalid {name}: the file does not exist")
            return EXIT_CONFIG_ERROR
        paths[name] = Path(value)
    # Request lines carry presigned URLs: keep httpx below INFO whatever else
    # configures logging.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    environ = dict(os.environ)
    # The SDK starts the CLI with os.environ overlaid by the options' env.
    scrub_process_environment(keep=["PATH"])
    run_dir = Path(tempfile.mkdtemp(prefix="srw-run-"))
    try:
        return asyncio.run(_serve(paths, environ, run_dir))
    except Exception as exc:  # run() never raises; a last resort for the loop
        _error(f"harness error: {type(exc).__name__}")
        return EXIT_HARNESS_ERROR


if __name__ == "__main__":
    sys.exit(main())
