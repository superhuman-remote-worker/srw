"""Entrypoint: ``python3 -m srw_claude_code_harness``, a drop-in SRW agent image.

The process serves the v1 harness routes (:mod:`.server`) on ``AGENT_PORT``,
registers with the orchestrator, takes one job over ``POST /job/start``, runs
it with Claude Code against the job's SSH workspace (:mod:`.runner`), reports
it on ``/api/jobs/{id}/complete``, deregisters and exits: one pod, one job.

Heartbeats: one right after registration (``ready``; the orchestrator
dispatches on the transition into ready), one right after a job is accepted
(``working``, with the job id), ``draining`` before every exit after
registration, and otherwise one per registered interval. A drain intent is
honoured while idle and ignored while the job runs (the pod ends with it).

Exit codes:

- ``EXIT_OK`` (0): the job was reported; or cancelled (the orchestrator has
  already recorded the cancel, so no report follows); or, while idle, a drain
  intent or no job within ``IDLE_EXIT_SECONDS`` of the first ready heartbeat
  (a custom pod is exempt from SRW's idle scale-down, so it ends itself).
- ``EXIT_HARNESS_ERROR`` (1): the completion report was refused or failed, or
  the runner raised (the job is then reported as a ``job_error``).
- ``EXIT_CONFIG_ERROR`` (2): ``ORCHESTRATOR_URL`` or ``MCP_INTERNAL_KEY`` is
  not set, or ``AGENT_PORT`` is not a port number.
- ``EXIT_INTERRUPTED`` (4): SIGTERM or SIGINT. A running job is stopped and
  waited for; no report follows.

Progress lines go to stdout as ``[srw-harness] <what>``. Nothing printed
contains the internal key, the model key or the delivery proof.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import signal
import socket
import sys
import tempfile
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx
import uvicorn

from . import __version__ as HARNESS_VERSION
from .job import JobSpec
from .orchestrator import OrchestratorClient, PinnedEcho, Registration
from .runner import (
    JobRunner,
    error_report,
    progress,
    redact,
    spec_secrets,
    warn,
)
from .server import HarnessState, create_app
from .session import scrub_process_environment
from .workspace import Workspace

EXIT_OK = 0
EXIT_HARNESS_ERROR = 1
EXIT_CONFIG_ERROR = 2
EXIT_INTERRUPTED = 4

DEFAULT_PORT = 8001
DEFAULT_CONFIG_NAME = "worker_base"
# No /job/start within this long of the first ready heartbeat: drain and exit.
IDLE_EXIT_SECONDS = 600.0
REGISTER_RETRY_SECONDS = 5.0
# After SIGTERM, how long a running job gets to stop (the pod has ~30 s).
TERMINATE_WAIT_SECONDS = 20.0
# After SIGTERM, the limit on each of the two leave calls (the draining
# heartbeat and the deregister), so the exit stays inside the pod's grace.
LEAVE_TIMEOUT_SECONDS = 3.0
SERVER_CLOSE_SECONDS = 5

StartServer = Callable[[Any, int], Awaitable[Callable[[], Awaitable[None]]]]


class _ConfigError(Exception):
    pass


@dataclass(frozen=True)
class Settings:
    """The pod environment the harness runs on, read once at start."""

    orchestrator_url: str
    internal_key: str = field(repr=False)
    port: int
    pod_uid: str
    build_sha: str
    image: str
    hostname: str
    pod_ip: str
    config_name: str
    path: str


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


def _detect_pod_ip() -> str:
    """The address the orchestrator reaches this pod on, as SRW's agent finds it:
    the source address of a route to the outside (no packet is sent)."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as probe:
            probe.connect(("8.8.8.8", 80))
            return probe.getsockname()[0]
    except Exception:  # noqa: BLE001 - no route: fall back to loopback
        return "127.0.0.1"


def _settings(environ: dict[str, str]) -> Settings:
    for name in ("ORCHESTRATOR_URL", "MCP_INTERNAL_KEY"):
        if not environ.get(name):
            raise _ConfigError(f"{name} is not set")
    raw_port = environ.get("AGENT_PORT") or str(DEFAULT_PORT)
    try:
        port = int(raw_port)
    except ValueError:
        port = 0
    if not 1 <= port <= 65535:
        raise _ConfigError("AGENT_PORT must be a port number")
    return Settings(
        orchestrator_url=environ["ORCHESTRATOR_URL"],
        internal_key=environ["MCP_INTERNAL_KEY"],
        port=port,
        pod_uid=environ.get("POD_UID", ""),
        build_sha=environ.get("BUILD_SHA", ""),
        image=environ.get("AGENT_IMAGE", ""),
        hostname=environ.get("HOSTNAME") or socket.gethostname(),
        pod_ip=environ.get("AGENT_POD_IP") or _detect_pod_ip(),
        config_name=environ.get("AGENT_CONFIG") or DEFAULT_CONFIG_NAME,
        path=environ.get("PATH") or os.defpath,
    )


def _describe(exc: BaseException) -> str:
    if isinstance(exc, httpx.HTTPStatusError):
        return f"HTTP {exc.response.status_code}"
    text = str(exc)
    return (f"{type(exc).__name__}: {text}" if text else type(exc).__name__)[:300]


async def _wait_any(events: list[asyncio.Event], timeout: float) -> None:
    """Return when any of ``events`` is set or ``timeout`` seconds have passed."""
    waiters = [asyncio.ensure_future(event.wait()) for event in events]
    try:
        await asyncio.wait(
            waiters, timeout=max(0.0, timeout), return_when=asyncio.FIRST_COMPLETED
        )
    finally:
        for waiter in waiters:
            waiter.cancel()
        await asyncio.gather(*waiters, return_exceptions=True)


class _Server(uvicorn.Server):
    @contextlib.contextmanager
    def capture_signals(self):  # type: ignore[override]
        # The harness handles SIGTERM and SIGINT itself (they stop the job
        # first); uvicorn must neither take them over nor re-raise them on exit.
        yield


async def _start_uvicorn(app: Any, port: int) -> Callable[[], Awaitable[None]]:
    """Serve ``app`` on all interfaces; returns the coroutine function that
    stops the server."""
    config = uvicorn.Config(
        app,
        host="0.0.0.0",
        port=port,
        lifespan="off",
        ws="none",
        log_level="warning",
        access_log=False,
        timeout_graceful_shutdown=SERVER_CLOSE_SECONDS,
    )
    server = _Server(config)
    serving = asyncio.create_task(server.serve())
    while not server.started:
        if serving.done():
            serving.result()  # the server's own failure, if it raised one
            raise RuntimeError(f"the HTTP server did not start on port {port}")
        await asyncio.sleep(0.05)

    async def close() -> None:
        server.should_exit = True
        await asyncio.wait({serving}, timeout=SERVER_CLOSE_SECONDS + 1)

    return close


class _Harness:
    def __init__(
        self,
        settings: Settings,
        *,
        http: httpx.AsyncClient,
        run_dir: Path,
        terminate: asyncio.Event,
        connect: Any,
        client_factory: Any,
    ) -> None:
        self.settings = settings
        self.terminate = terminate
        self.client = OrchestratorClient(
            settings.orchestrator_url, settings.internal_key, http=http, warn=warn
        )
        self.state = HarnessState(pod_uid=settings.pod_uid)
        self.state.run_job = self._run_job
        self.state.report_rejection = self._report_rejection
        # One stop for the session: SRW's cancel and the signals both set it.
        self.runner = JobRunner(
            client=self.client,
            pod_uid=settings.pod_uid,
            run_dir=run_dir,
            stop=self.state.stop_requested,
            connect=connect,
            client_factory=client_factory,
            path=settings.path,
        )
        self.app = create_app(self.state)
        self.accepted = asyncio.Event()
        self.intake_closed = False
        self.code = EXIT_OK

    # -- the job ---------------------------------------------------------------

    def _close_intake(self) -> None:
        """Single-use pod: from now on ``/job/start`` is refused and ``/ready``
        says not ready."""
        self.intake_closed = True
        self.state.agent_id = None

    def _adopt_registration(self) -> None:
        """Answer to the client's current registration.

        A heartbeat answered 404 makes the client register again under a new
        agent id and process generation, and the orchestrator addresses the
        next ``/job/start`` to those. Adopted only while no job has been
        accepted: the accepted job's cancel still names the identity it was
        dispatched to, and a closed intake stays closed.
        """
        registration = self.client.registration
        if (
            registration is None
            or self.intake_closed
            or self.state.current_job_id is not None
        ):
            return
        if registration.agent_id != self.state.agent_id:
            progress(f"registered again as agent {registration.agent_id}")
        self.state.process_generation = registration.dispatch_process_generation
        self.state.agent_id = registration.agent_id

    async def _run_job(self, spec: JobSpec) -> None:
        self.accepted.set()
        try:
            progress(f"job {spec.job_id} accepted")
            report = await self.runner.run(spec)
            if report is None or self.state.stop_requested.is_set():
                progress(f"job {spec.job_id} stopped: no completion report")
                return
            await self._send(spec.job_id, report, spec.pinned)
        except Exception as exc:
            self.code = EXIT_HARNESS_ERROR
            message = redact(_describe(exc), spec_secrets(spec))
            warn(f"harness error: {message}")
            if not self.state.stop_requested.is_set():
                with contextlib.suppress(Exception):
                    await self._send(spec.job_id, error_report(message), spec.pinned)
        finally:
            self._close_intake()

    async def _report_rejection(
        self, job_id: str, pinned: PinnedEcho | None, message: str
    ) -> None:
        self.accepted.set()
        try:
            progress(f"job {job_id} refused: {message}")
            await self._send(job_id, error_report(message), pinned)
        except Exception as exc:
            self.code = EXIT_HARNESS_ERROR
            warn(f"reporting job {job_id} failed: {type(exc).__name__}")
        finally:
            self._close_intake()

    async def _send(self, job_id: str, report: Any, pinned: PinnedEcho | None) -> None:
        if await self.runner.complete(job_id, report, pinned=pinned):
            progress(f"job {job_id} reported")
        else:
            self.code = EXIT_HARNESS_ERROR
            warn(f"the completion report for job {job_id} was not accepted")

    # -- the pod's lifecycle -----------------------------------------------------

    async def _register(self) -> Registration | None:
        s = self.settings
        while not self.terminate.is_set():
            try:
                registration = await self.client.register(
                    config_name=s.config_name,
                    pod_ip=s.pod_ip,
                    pod_port=s.port,
                    hostname=s.hostname,
                    build_sha=s.build_sha,
                    pod_uid=s.pod_uid,
                    image=s.image,
                )
            except Exception as exc:  # noqa: BLE001 - retried until it works
                warn(
                    f"registration failed ({_describe(exc)}); "
                    f"retrying in {REGISTER_RETRY_SECONDS:g} s"
                )
                await _wait_any([self.terminate], REGISTER_RETRY_SECONDS)
                continue
            self.state.process_generation = registration.dispatch_process_generation
            self.state.agent_id = registration.agent_id
            progress(f"registered as agent {registration.agent_id}")
            return registration
        return None

    async def _leave(
        self, code: int, reason: str, *, call_timeout: float | None = None
    ) -> int:
        progress(reason)
        self._close_intake()
        limit = {} if call_timeout is None else {"timeout": call_timeout}
        with contextlib.suppress(Exception):
            await self.client.heartbeat("draining", current_job_id=None, **limit)
        with contextlib.suppress(Exception):
            await self.client.deregister(**limit)
        progress(f"exit {code}")
        return code

    async def _terminated(self) -> int:
        self.state.stop_requested.set()
        if (
            self.state.current_job_id is not None
            and not self.state.job_finished.is_set()
        ):
            progress("stopping the job")
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(
                    self.state.job_finished.wait(), TERMINATE_WAIT_SECONDS
                )
        return await self._leave(
            EXIT_INTERRUPTED,
            "stopped by a signal",
            call_timeout=LEAVE_TIMEOUT_SECONDS,
        )

    async def lifecycle(self) -> int:
        registration = await self._register()
        if registration is None:
            progress("stopped by a signal before registering")
            return EXIT_INTERRUPTED
        loop = asyncio.get_running_loop()
        interval = max(1.0, float(registration.heartbeat_interval_seconds))
        sent: str | None = None
        next_beat = loop.time()
        idle_deadline: float | None = None
        while True:
            if self.state.job_finished.is_set():
                return await self._leave(self.code, "the job has ended")
            if self.terminate.is_set():
                return await self._terminated()
            job_id = self.state.current_job_id
            status = "working" if job_id is not None else "ready"
            if status != sent or loop.time() >= next_beat:
                intents = await self.client.heartbeat(status, current_job_id=job_id)
                self._adopt_registration()
                sent, next_beat = status, loop.time() + interval
                if idle_deadline is None:
                    idle_deadline = loop.time() + IDLE_EXIT_SECONDS
                if (
                    status == "ready"
                    and isinstance(intents, dict)
                    and intents.get("should_drain")
                    and self.state.current_job_id is None
                ):
                    reason = intents.get("drain_reason") or "unspecified"
                    return await self._leave(EXIT_OK, f"drain requested ({reason})")
                continue
            now = loop.time()
            if job_id is None and now >= idle_deadline:
                return await self._leave(
                    EXIT_OK, f"no job within {IDLE_EXIT_SECONDS:g} s: exiting"
                )
            wake = next_beat if job_id is not None else min(next_beat, idle_deadline)
            events = [self.terminate, self.state.job_finished]
            if not self.accepted.is_set():
                events.append(self.accepted)
            await _wait_any(events, wake - now)


async def _serve(
    settings: Settings,
    *,
    terminate: asyncio.Event,
    run_dir: Path,
    http: httpx.AsyncClient | None = None,
    start_server: StartServer = _start_uvicorn,
    connect: Any = Workspace.connect,
    client_factory: Any = None,
) -> int:
    """Serve, register, run at most one job and exit; returns the exit code.

    ``terminate`` is set by SIGTERM/SIGINT. ``http`` (the orchestrator client),
    ``start_server``, ``connect`` and ``client_factory`` are replaced by tests.
    """
    own_http = http is None
    if http is None:
        http = httpx.AsyncClient()
    try:
        harness = _Harness(
            settings,
            http=http,
            run_dir=run_dir,
            terminate=terminate,
            connect=connect,
            client_factory=client_factory,
        )
        close = await start_server(harness.app, settings.port)
        progress(f"serving on port {settings.port}")
        try:
            return await harness.lifecycle()
        finally:
            await close()
    finally:
        if own_http:
            await http.aclose()


async def _main(settings: Settings, run_dir: Path) -> int:
    loop = asyncio.get_running_loop()
    terminate = asyncio.Event()

    def request_stop(sig: signal.Signals) -> None:
        if not terminate.is_set():
            progress(f"{sig.name} received: stopping")
            terminate.set()

    installed = []
    for sig in (signal.SIGTERM, signal.SIGINT):
        with contextlib.suppress(NotImplementedError, RuntimeError, ValueError):
            loop.add_signal_handler(sig, request_stop, sig)
            installed.append(sig)
    try:
        return await _serve(settings, terminate=terminate, run_dir=run_dir)
    finally:
        for sig in installed:
            loop.remove_signal_handler(sig)


def main(argv: list[str] | None = None) -> int:
    """Run the harness from the pod environment; returns the exit code.

    Takes no arguments (``argv`` is accepted and ignored). The environment is
    read once and then scrubbed down to ``PATH`` and the local user names the
    image sets (``USER``, ``LOGNAME``; asyncssh needs them when the pod's uid
    has no passwd entry, and they are not secrets): the SDK starts the CLI with
    ``os.environ`` overlaid by the options' env, and the pod's Secret must not
    reach it.
    """
    del argv
    environ = dict(os.environ)
    try:
        settings = _settings(environ)
    except _ConfigError as exc:
        warn(str(exc))
        return EXIT_CONFIG_ERROR
    logging.getLogger("httpx").setLevel(logging.WARNING)
    scrub_process_environment(keep=["PATH", "USER", "LOGNAME"])
    run_dir = Path(tempfile.mkdtemp(prefix="srw-run-"))
    try:
        return asyncio.run(_main(settings, run_dir))
    except Exception as exc:  # a last resort: the lifecycle handles its own faults
        warn(f"harness error: {type(exc).__name__}")
        return EXIT_HARNESS_ERROR


if __name__ == "__main__":
    sys.exit(main())
