"""Run the built ``srw`` binary against real routers on a loopback port.

No fixture in this suite serves the orchestrator over TCP, so this builds one
from parts that each have a precedent here: a ``PostgresContainer`` loaded
with ``schema_current.sql``, the real routers mounted with
``tests/_mounted_router.py::mount_router`` and their real dependencies, the
real token resolver with a personal access token stored in ``auth_tokens``,
and ``uvicorn.Server`` with ``lifespan="off"``. The full application lifespan
is never started.

The server, its database pool and every seeding call share one event loop in
one thread, because an asyncpg pool belongs to the loop that created it. The
tests themselves are synchronous: they run the binary as a subprocess.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import hashlib
import os
from pathlib import Path
import re
import shutil
import socket
import subprocess
import threading
from uuid import uuid4

import asyncpg
import uvicorn

REPO_ROOT = Path(__file__).resolve().parent.parent
CLI_DIR = REPO_ROOT / "cli"
SCHEMA = REPO_ROOT / "src/orchestrator/database/schema_current.sql"


def build_binary(directory: Path) -> Path:
    """Build ``srw`` into ``directory``. A missing Go toolchain skips the
    module on a developer's machine and fails it where ``CI`` is set."""
    import pytest

    go = shutil.which("go")
    if go is None:
        if os.environ.get("CI"):
            raise AssertionError("the Go toolchain is missing in CI")
        pytest.skip("the Go toolchain is not installed")
    binary = directory / "srw"
    subprocess.run(
        [go, "build", "-o", str(binary), "."],
        cwd=CLI_DIR,
        check=True,
        env={**os.environ, "CGO_ENABLED": "0"},
    )
    return binary


@dataclass
class Result:
    code: int
    out: str
    err: str


class Installation:
    """A running server with its database, reachable from the test thread."""

    def __init__(self, postgres_url: str):
        self._postgres_url = postgres_url
        self._ready = threading.Event()
        self._failure: BaseException | None = None
        self.url = ""
        self.db = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._server: uvicorn.Server | None = None
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> "Installation":
        self._thread.start()
        self._ready.wait(180)
        if self._failure is not None:
            raise self._failure
        if not self.url:
            raise AssertionError("the wire-test server did not start")
        return self

    def stop(self) -> None:
        if self._server is not None:
            self._server.should_exit = True
        self._thread.join(30)

    def _run(self) -> None:
        try:
            asyncio.run(self._serve())
        except BaseException as error:  # noqa: BLE001 - reported to the test thread
            self._failure = error
            self._ready.set()

    async def _serve(self) -> None:
        from orchestrator.database.postgres import PostgresDB

        name = "srw_cli_wire_" + uuid4().hex
        admin = await asyncpg.connect(self._postgres_url)
        await admin.execute(f'CREATE DATABASE "{name}"')
        await admin.close()
        db = PostgresDB(self._postgres_url.rsplit("/", 1)[0] + "/" + name)
        await db.connect()
        await db.execute(SCHEMA.read_text())
        db.manifest_runtime_image = "test.invalid/srw:installed"
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        server = uvicorn.Server(
            uvicorn.Config(
                _application(db),
                host="127.0.0.1",
                port=port,
                log_level="warning",
                lifespan="off",
            )
        )
        self.db, self._loop, self._server = db, asyncio.get_running_loop(), server
        task = asyncio.create_task(server.serve())
        while not server.started and not task.done():
            await asyncio.sleep(0.05)
        self.url = f"http://127.0.0.1:{port}"
        self._ready.set()
        await task
        await db.disconnect()

    def run(self, coroutine):
        """Run a coroutine on the server's loop (its database pool lives there)."""
        return asyncio.run_coroutine_threadsafe(coroutine, self._loop).result(60)

    def user(self, *, admin: bool = False, name: str = "Wire test user") -> str:
        row = self.run(
            self.db.fetchrow(
                "INSERT INTO users(display_name,email,is_approved,is_admin) "
                "VALUES($1,$2,TRUE,$3) RETURNING id",
                name,
                f"{uuid4().hex[:8]}@example.test",
                admin,
            )
        )
        return str(row["id"])

    def token(self, user_id: str, *scopes: str, name: str = "wire test") -> str:
        """A personal access token as the token routes store one."""
        token = "ak_" + uuid4().hex + uuid4().hex
        self.run(
            self.db.execute(
                "INSERT INTO auth_tokens (user_id, name, token_hash, token_prefix, "
                "kind, scopes, expires_at) VALUES ($1::uuid, $2, $3, $4, 'api', $5, "
                "now() + interval '90 days')",
                user_id,
                name,
                hashlib.sha256(token.encode("ascii")).hexdigest(),
                token[:12],
                list(scopes),
            )
        )
        return token


def _application(db):
    from orchestrator.routers import identity as identity_routes
    from orchestrator.routers import manifests as manifest_routes
    from orchestrator.routers import version as version_routes
    from orchestrator.security import auth
    from orchestrator.services.manifest_resources import ManifestResourceService
    from orchestrator.services.manifests import ManifestService
    from tests._mounted_router import mount_router

    async def approved_user(request):
        return await auth.require_approved_user(request, db)

    manifests = manifest_routes.ManifestDependencies(
        service=ManifestService(),
        require_approved_user=approved_user,
        resources=ManifestResourceService(db),
    )
    return mount_router(
        manifest_routes.router,
        identity_routes.router,
        version_routes.router,
        factories={
            "manifest_dependencies_factory": lambda: manifests,
            "identity_dependencies_factory": lambda: (
                identity_routes.IdentityDependencies(store=db)
            ),
            "version_dependencies_factory": lambda: (
                version_routes.VersionDependencies(store=db)
            ),
        },
    )


class Srw:
    """The binary, in environment mode against one installation."""

    def __init__(self, binary: Path, installation: Installation, home: Path):
        self.binary, self.installation, self.home = binary, installation, home

    def __call__(self, token: str, *args: str, stdin: str = "") -> Result:
        done = subprocess.run(
            [str(self.binary), *args],
            input=stdin,
            capture_output=True,
            text=True,
            timeout=120,
            env={
                "HOME": str(self.home),
                "PATH": os.environ.get("PATH", ""),
                "SRW_API_URL": self.installation.url,
                "SRW_TOKEN": token,
            },
        )
        assert token not in done.stdout + done.stderr, "the token was printed"
        return Result(done.returncode, done.stdout, done.stderr)


def postgres_url(container) -> str:
    return re.sub(
        r"^postgresql\+\w+://", "postgresql://", container.get_connection_url()
    )
