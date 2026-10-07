"""Golden pins: ``POST /api/datasources/{id}/test`` for every connector type.

Each case resolves one stored row through the real router and service and
records the HTTP status, the body, and what the probe asked of the library it
drives. Nothing touches the network: every probe is mocked at the seam the
existing tests use (``tests/test_codeql_error_disclosure.py``,
``tests/test_repository_probe.py``, ``tests/test_kb_datasource_api.py``,
``tests/test_mcp_datasource_api.py``):

* PostgreSQL: ``orchestrator.services.datasources.asyncpg.connect``;
* Neo4j, MongoDB, WebDAV: the ``neo4j``, ``pymongo`` and ``webdav3.client``
  modules, replaced in ``sys.modules`` (they are imported inside the probe);
* email: ``imaplib``/``smtplib`` classes, or ``probe_email_connection`` for the
  outer timeout and crash classes;
* repository: ``shared.runtime.services.forge._transport``;
* KB: ``orchestrator.services.kb_datasources.kb_source_from_datasource``;
* MCP: the SDK's transport clients and ``ClientSession``.

``error_ref`` values are random and normalised to ``<error_ref>``.

Regenerate: ``UPDATE_CONNECTOR_GOLDENS=1 python -m pytest
tests/test_connector_goldens_probe.py`` (see ``tests/_connector_goldens.py``).
"""

from __future__ import annotations

import copy
import imaplib
import shutil
import smtplib
import sys
from collections.abc import Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import httpx
import pytest
from fastapi.testclient import TestClient

from tests._connector_goldens import USER, Golden, resolved_row
from tests._mounted_router import mount_router

Recorder = list[Any]
#: A scenario installs its mocks and returns nothing; it records into ``calls``.
Scenario = Callable[[pytest.MonkeyPatch, Recorder], None]


@dataclass(frozen=True)
class ProbeCase:
    row: dict[str, Any]
    scenario: Scenario | None = None
    mcp: bool = True
    stdio: bool = True
    gate_crashes: bool = False
    pinned_defect: str | None = None


def _stored(kind: str, **over: Any) -> dict[str, Any]:
    row = resolved_row(kind, **over)
    row.pop("project_read_only")
    return row


# =============================================================================
# Scenarios
# =============================================================================


def postgres(*, error: Exception | None = None) -> Scenario:
    def install(monkeypatch, calls):
        from orchestrator.services import datasources

        class Connection:
            async def fetchval(self, query):
                calls.append({"fetchval": query})
                return (
                    "PostgreSQL 16.4 (Debian 16.4-1.pgdg120+2) on x86_64-pc-linux-gnu, "
                    "compiled by gcc (Debian 12.2.0-14) 12.2.0, 64-bit"
                )

            async def close(self):
                calls.append("close")

        async def connect(url, **kwargs):
            calls.append({"asyncpg.connect": [url, kwargs]})
            if error is not None:
                raise error
            return Connection()

        monkeypatch.setattr(datasources.asyncpg, "connect", connect)

    return install


def neo4j(*, error: Exception | None = None) -> Scenario:
    def install(monkeypatch, calls):
        class Driver:
            def verify_connectivity(self):
                calls.append("verify_connectivity")
                if error is not None:
                    raise error

            def close(self):
                calls.append("close")

        def driver(url, auth):
            calls.append({"neo4j.GraphDatabase.driver": [url, list(auth)]})
            return Driver()

        module = SimpleNamespace(GraphDatabase=SimpleNamespace(driver=driver))
        monkeypatch.setitem(sys.modules, "neo4j", module)

    return install


def mongodb(*, error: Exception | None = None) -> Scenario:
    def install(monkeypatch, calls):
        class MongoClient:
            def __init__(self, url, **kwargs):
                calls.append({"pymongo.MongoClient": [url, kwargs]})

            def server_info(self):
                calls.append("server_info")
                if error is not None:
                    raise error
                return {"version": "7.0.0"}

            def close(self):
                calls.append("close")

        monkeypatch.setitem(
            sys.modules, "pymongo", SimpleNamespace(MongoClient=MongoClient)
        )

    return install


def webdav(*, error: Exception | None = None) -> Scenario:
    def install(monkeypatch, calls):
        class Client:
            def __init__(self, options):
                calls.append({"webdav3.client.Client": options})

            def list(self, path):
                calls.append({"list": path})
                if error is not None:
                    raise error
                return []

        monkeypatch.setitem(
            sys.modules, "webdav3.client", SimpleNamespace(Client=Client)
        )

    return install


def mail(
    *,
    connect_error: Exception | None = None,
    login_error: Exception | None = None,
    missing_folders: tuple[str, ...] = (),
    smtp_error: Exception | None = None,
) -> Scenario:
    def install(monkeypatch, calls):
        imap_error = imaplib.IMAP4.error

        def imap_class(name):
            class Imap:
                # The probe catches ``imaplib.IMAP4.error``; keep the real one.
                error = imap_error

                def __init__(self, host, port, timeout):
                    calls.append({f"imaplib.{name}": [host, port, timeout]})
                    if connect_error is not None:
                        raise connect_error

                def starttls(self):
                    calls.append("imap.starttls")

                def login(self, username, password):
                    calls.append({"imap.login": username})
                    if login_error is not None:
                        raise login_error

                def status(self, mailbox, items):
                    calls.append({"imap.status": [mailbox, items]})
                    if mailbox.strip('"') in missing_folders:
                        return "NO", [b"Mailbox does not exist"]
                    return "OK", [b"(MESSAGES 3)"]

                def logout(self):
                    calls.append("imap.logout")

            return Imap

        def smtp_class(name):
            class Smtp:
                def __init__(self, host, port, timeout):
                    calls.append({f"smtplib.{name}": [host, port, timeout]})

                def starttls(self):
                    calls.append("smtp.starttls")

                def ehlo(self):
                    calls.append("smtp.ehlo")

                def login(self, username, password):
                    calls.append({"smtp.login": username})
                    if smtp_error is not None:
                        raise smtp_error

                def quit(self):
                    calls.append("smtp.quit")

            return Smtp

        monkeypatch.setattr(imaplib, "IMAP4_SSL", imap_class("IMAP4_SSL"))
        monkeypatch.setattr(imaplib, "IMAP4", imap_class("IMAP4"))
        monkeypatch.setattr(smtplib, "SMTP_SSL", smtp_class("SMTP_SSL"))
        monkeypatch.setattr(smtplib, "SMTP", smtp_class("SMTP"))

    return install


def mail_probe_raises(error: Exception) -> Scenario:
    def install(monkeypatch, calls):
        from orchestrator.services import datasources

        def probe(credentials, config):
            calls.append("probe_email_connection")
            raise error

        monkeypatch.setattr(datasources, "probe_email_connection", probe)

    return install


def forge(
    *, user: dict | int, repo: dict | int, user_headers: dict | None = None
) -> Scenario:
    def install(monkeypatch, calls):
        from shared.runtime.services import forge as forge_module

        def handler(request: httpx.Request) -> httpx.Response:
            calls.append(
                {
                    "request": f"{request.method} {request.url}",
                    "authorization": bool(request.headers.get("authorization")),
                }
            )
            if request.url.path.endswith("/user"):
                if isinstance(user, int):
                    return httpx.Response(user, json={"message": "Bad credentials"})
                return httpx.Response(200, json=user, headers=user_headers or {})
            if isinstance(repo, int):
                return httpx.Response(repo, json={"message": "Not Found"})
            return httpx.Response(200, json=repo)

        monkeypatch.setattr(forge_module, "_transport", httpx.MockTransport(handler))

    return install


def forge_unreachable() -> Scenario:
    def install(monkeypatch, calls):
        from shared.runtime.services import forge as forge_module

        def handler(request: httpx.Request) -> httpx.Response:
            calls.append({"request": f"{request.method} {request.url}"})
            raise httpx.ConnectError("connection refused", request=request)

        monkeypatch.setattr(forge_module, "_transport", httpx.MockTransport(handler))

    return install


def kb_source(
    *,
    head: str | None = "c" * 40,
    tree: list | None = None,
    error: Exception | None = None,
) -> Scenario:
    def install(monkeypatch, calls):
        from orchestrator.services import kb_datasources

        class Source:
            async def get_head(self):
                calls.append("get_head")
                return head

            @asynccontextmanager
            async def snapshot(self, ref):
                calls.append({"snapshot": ref})

                async def list_tree():
                    return list(tree or [])

                yield SimpleNamespace(list_tree=list_tree)

        def build(datasource):
            calls.append({"kb_source_from_datasource": datasource["connection_url"]})
            if error is not None:
                raise error
            return Source()

        monkeypatch.setattr(kb_datasources, "kb_source_from_datasource", build)

    return install


def mcp_sdk(
    *, tools: tuple[str, ...] = (), error: BaseException | None = None
) -> Scenario:
    """Fake the SDK transports and session; ``error`` is raised on connect."""

    def install(monkeypatch, calls):
        import mcp
        import mcp.client.sse
        import mcp.client.stdio
        import mcp.client.streamable_http

        def opened(record):
            calls.append(record)
            if error is not None:
                raise error

        @asynccontextmanager
        async def http_client_cm(headers=None, **_kwargs):
            yield SimpleNamespace(headers=dict(headers or {}))

        @asynccontextmanager
        async def streamable_http_client(url, http_client=None, **_kwargs):
            opened(
                {
                    "transport": "streamable_http",
                    "url": url,
                    "headers": http_client.headers,
                }
            )
            yield "read", "write", None

        @asynccontextmanager
        async def streamablehttp_client(url, headers=None, **_kwargs):
            opened(
                {
                    "transport": "streamable_http",
                    "url": url,
                    "headers": dict(headers or {}),
                }
            )
            yield "read", "write", None

        @asynccontextmanager
        async def sse_client(url, headers=None, **_kwargs):
            opened({"transport": "sse", "url": url, "headers": dict(headers or {})})
            yield "read", "write"

        @asynccontextmanager
        async def stdio_client(parameters, errlog=None):
            opened(
                {
                    "transport": "stdio",
                    "command": parameters.command,
                    "args": list(parameters.args),
                    "credential_env": {
                        key: parameters.env[key]
                        for key in ("ACME_KEY",)
                        if key in (parameters.env or {})
                    },
                    "stderr_discarded": getattr(errlog, "name", None) == "/dev/null",
                }
            )
            yield "read", "write"

        class Session:
            def __init__(self, read, write):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, *exc):
                return False

            async def initialize(self):
                calls.append("initialize")

            async def list_tools(self):
                calls.append("list_tools")
                return SimpleNamespace(
                    tools=[SimpleNamespace(name=name) for name in tools]
                )

        streamable = mcp.client.streamable_http
        if hasattr(streamable, "streamable_http_client"):
            import mcp.shared._httpx_utils

            monkeypatch.setattr(
                streamable, "streamable_http_client", streamable_http_client
            )
            monkeypatch.setattr(
                mcp.shared._httpx_utils, "create_mcp_http_client", http_client_cm
            )
        else:
            monkeypatch.setattr(
                streamable, "streamablehttp_client", streamablehttp_client
            )
        monkeypatch.setattr(mcp.client.sse, "sse_client", sse_client)
        monkeypatch.setattr(mcp.client.stdio, "stdio_client", stdio_client)
        monkeypatch.setattr(mcp, "ClientSession", Session)
        monkeypatch.setattr(shutil, "which", lambda command: f"/usr/bin/{command}")

    return install


def which_finds_nothing() -> Scenario:
    def install(monkeypatch, calls):
        monkeypatch.setattr(shutil, "which", lambda command: None)

    return install


# =============================================================================
# Cases
# =============================================================================

_SEND_CONFIG = {
    "access": "send",
    "folders": ["INBOX", "Support"],
    "drafts_folder": "Drafts",
    "from_address": "support@example.test",
    "recipient_allowlist": [],
    "unattended_send": False,
}
_UNKNOWN_TYPE = (
    "D1 fixes (L1 §6 #1): credential-file connectors answer Test with "
    "'Unknown connector type' while the cockpit offers Test on every row"
)
_GITHUB_REPO = {"default_branch": "main", "permissions": {"push": True}}

CASES: dict[str, ProbeCase] = {
    "gate_crash_is_a_500": ProbeCase(_stored("generic"), gate_crashes=True),
    # ---- env connectors ----------------------------------------------------
    "generic/no_test": ProbeCase(_stored("generic")),
    "credentials/valid": ProbeCase(_stored("credentials")),
    "credentials/invalid_stored_env_is_a_500": ProbeCase(
        _stored("credentials", credentials={"env_vars": {"PATH": "x"}})
    ),
    # ---- credential files --------------------------------------------------
    "generic_file/unknown_type": ProbeCase(
        _stored("generic_file"), pinned_defect=_UNKNOWN_TYPE
    ),
    "kubeconfig/unknown_type": ProbeCase(
        _stored("kubeconfig"), pinned_defect=_UNKNOWN_TYPE
    ),
    "ssh_key/unknown_type": ProbeCase(_stored("ssh_key"), pinned_defect=_UNKNOWN_TYPE),
    # ---- managed databases -------------------------------------------------
    "postgresql/connected": ProbeCase(_stored("postgresql"), postgres()),
    "postgresql/connect_failed": ProbeCase(
        _stored("postgresql"), postgres(error=OSError("could not connect to pg-secret"))
    ),
    "neo4j/connected": ProbeCase(
        _stored("neo4j"),
        neo4j(),
        pinned_defect="D1 names it (L1 §6 #8): the Neo4j probe blocks the event loop",
    ),
    "neo4j/no_credentials_default_auth": ProbeCase(
        _stored("neo4j", credentials={}), neo4j()
    ),
    "neo4j/connect_failed": ProbeCase(
        _stored("neo4j"), neo4j(error=RuntimeError("auth failure for neo-secret"))
    ),
    "mongodb/connected": ProbeCase(
        _stored("mongodb"),
        mongodb(),
        pinned_defect="D1 names it (L1 §6 #8): the MongoDB probe blocks the event loop",
    ),
    "mongodb/connect_failed": ProbeCase(
        _stored("mongodb"), mongodb(error=RuntimeError("mongo-secret timeout"))
    ),
    "webdav/connected": ProbeCase(
        _stored("webdav"),
        webdav(),
        pinned_defect="D1 names it (L1 §6 #8): the WebDAV probe blocks the event loop",
    ),
    "webdav/connect_failed": ProbeCase(
        _stored("webdav"), webdav(error=RuntimeError("401 for dav-secret"))
    ),
    # ---- email -------------------------------------------------------------
    "email/draft_connected": ProbeCase(_stored("email"), mail()),
    "email/send_connected_starttls": ProbeCase(
        _stored(
            "email",
            config=_SEND_CONFIG,
            credentials={
                "username": "support@example.test",
                "password": "mail-secret",
                "imap": {"host": "imap.example.test", "security": "starttls"},
                "smtp": {"host": "smtp.example.test", "security": "starttls"},
            },
        ),
        mail(),
    ),
    "email/imap_connect_failed": ProbeCase(
        _stored("email"), mail(connect_error=OSError("Connection refused"))
    ),
    "email/imap_login_failed": ProbeCase(
        _stored("email"),
        mail(
            login_error=imaplib.IMAP4.error(
                "[AUTHENTICATIONFAILED] Invalid credentials"
            )
        ),
    ),
    "email/folder_missing": ProbeCase(
        _stored("email"), mail(missing_folders=("Support",))
    ),
    "email/smtp_auth_failed": ProbeCase(
        _stored("email", config=_SEND_CONFIG),
        mail(smtp_error=smtplib.SMTPAuthenticationError(535, b"5.7.8 rejected")),
    ),
    "email/smtp_connect_failed": ProbeCase(
        _stored("email", config=_SEND_CONFIG),
        mail(
            smtp_error=smtplib.SMTPServerDisconnected("Connection unexpectedly closed")
        ),
    ),
    "email/send_without_folders": ProbeCase(
        _stored("email", config={**_SEND_CONFIG, "folders": []}), mail()
    ),
    "email/send_without_smtp": ProbeCase(
        _stored(
            "email",
            config=_SEND_CONFIG,
            credentials={
                "username": "support@example.test",
                "password": "mail-secret",
                "imap": {"host": "imap.example.test"},
            },
        ),
        mail(),
    ),
    "email/incomplete_credentials": ProbeCase(
        _stored("email", credentials={"username": "support@example.test"}), mail()
    ),
    "email/outer_timeout": ProbeCase(
        _stored("email"), mail_probe_raises(TimeoutError())
    ),
    "email/probe_crashed": ProbeCase(
        _stored("email"), mail_probe_raises(RuntimeError("mail-secret leaked"))
    ),
    # ---- repository --------------------------------------------------------
    "repository/token_write_access": ProbeCase(
        _stored("repository_token", default_branch="develop"),
        forge(user={"login": "srw-bot", "id": 5}, repo=_GITHUB_REPO),
    ),
    "repository/token_warnings": ProbeCase(
        _stored("repository_token"),
        forge(
            user={"login": "owner", "id": 5},
            user_headers={"X-OAuth-Scopes": "repo"},
            repo={
                "default_branch": "main",
                "permissions": {"admin": True, "push": False},
            },
        ),
    ),
    "repository/token_read_only_connector": ProbeCase(
        _stored("repository_token", read_only=True),
        forge(
            user={"login": "reader", "id": 6},
            repo={"default_branch": "main", "permissions": {"pull": True}},
        ),
    ),
    "repository/token_gitea": ProbeCase(
        _stored(
            "repository_token",
            connection_url="https://git.example.test/acme/widgets.git",
            config={"forge": "gitea"},
        ),
        forge(
            user={"login": "srw-bot", "id": 5},
            repo={"default_branch": "main", "permissions": {"push": True}},
        ),
    ),
    "repository/token_rejected": ProbeCase(
        _stored("repository_token"), forge(user=401, repo={})
    ),
    "repository/not_found": ProbeCase(
        _stored("repository_token"), forge(user={"login": "bot", "id": 1}, repo=404)
    ),
    "repository/unreachable": ProbeCase(
        _stored("repository_token"), forge_unreachable()
    ),
    "repository/self_hosted_without_forge": ProbeCase(
        _stored(
            "repository_token",
            connection_url="https://git.example.test/acme/widgets.git",
            config={},
        )
    ),
    "repository/ssh_key_not_probed": ProbeCase(_stored("repository_ssh")),
    "repository/no_credentials_not_probed": ProbeCase(
        _stored("repository_token", credentials={})
    ),
    "repository/token_inferred_without_auth_method": ProbeCase(
        _stored("repository_token", credentials={"token": "ghp_widgets-secret"}),
        forge(user={"login": "srw-bot", "id": 5}, repo=_GITHUB_REPO),
    ),
    # ---- kb ----------------------------------------------------------------
    "kb/notes_found": ProbeCase(
        _stored("kb"),
        kb_source(
            tree=[
                {"path": "vault/index.md", "type": "blob", "sha": "a"},
                {"path": "vault/a.md", "type": "blob", "sha": "b"},
                {"path": "vault/sub/b.md", "type": "blob", "sha": "c"},
                {"path": "vault/img.png", "type": "blob", "sha": "d"},
                {"path": "other/c.md", "type": "blob", "sha": "e"},
            ]
        ),
    ),
    "kb/no_head": ProbeCase(_stored("kb"), kb_source(head=None)),
    "kb/no_notes_under_root": ProbeCase(
        _stored("kb"), kb_source(tree=[{"path": "x.md", "type": "blob", "sha": "a"}])
    ),
    "kb/no_notes_without_root": ProbeCase(_stored("kb", config={}), kb_source(tree=[])),
    "kb/source_failed": ProbeCase(
        _stored("kb"), kb_source(error=ValueError("token kb-secret rejected"))
    ),
    # ---- mcp ---------------------------------------------------------------
    "mcp/gate_off": ProbeCase(_stored("mcp_remote"), mcp_sdk(), mcp=False),
    "mcp/invalid_stored_config": ProbeCase(
        _stored("mcp_remote", connection_url=None), mcp_sdk()
    ),
    "mcp/remote_http_connected": ProbeCase(
        _stored("mcp_remote"),
        mcp_sdk(tools=tuple(f"tool_{index}" for index in range(10))),
    ),
    "mcp/remote_sse_custom_headers_no_tools": ProbeCase(
        _stored(
            "mcp_remote",
            credentials={
                "transport": "sse",
                "auth": {"type": "headers", "headers": {"X-Api-Key": "k"}},
            },
        ),
        mcp_sdk(),
    ),
    "mcp/remote_connect_failed": ProbeCase(
        _stored("mcp_remote"), mcp_sdk(error=httpx.ConnectError("refused mcp-secret"))
    ),
    "mcp/remote_timed_out": ProbeCase(
        _stored("mcp_remote"), mcp_sdk(error=TimeoutError())
    ),
    "mcp/stdio_connected": ProbeCase(_stored("mcp_stdio"), mcp_sdk(tools=("echo",))),
    "mcp/stdio_runtime_missing": ProbeCase(_stored("mcp_stdio"), which_finds_nothing()),
    "mcp/stdio_gate_off": ProbeCase(_stored("mcp_stdio"), mcp_sdk(), stdio=False),
}


# =============================================================================
# Harness
# =============================================================================


def run_case(case: ProbeCase, monkeypatch) -> dict[str, Any]:
    from orchestrator.routers.datasources import DatasourcesDependencies, router
    from orchestrator.services import knowledge_index
    from orchestrator.services.datasource_config import validate_mcp_datasource
    from orchestrator.services.datasources import DatasourceDependencies
    from orchestrator.services.deployment_gates import mcp_datasources_enabled
    from orchestrator.services.kb_task_registry import KbDatasourceTaskRegistry

    for name, on in (
        ("MCP_DATASOURCES_ENABLED", case.mcp),
        ("MCP_STDIO_ENABLED", case.stdio),
    ):
        if on:
            monkeypatch.setenv(name, "true")
        else:
            monkeypatch.delenv(name, raising=False)

    calls: Recorder = []
    if case.scenario is not None:
        case.scenario(monkeypatch, calls)

    async def datasource_owner(_request, _store, _datasource_id):
        if case.gate_crashes:
            raise RuntimeError("gate exploded")
        return dict(USER), copy.deepcopy(case.row)

    store = MagicMock()
    dependencies = DatasourcesDependencies(
        store=store,
        operations=DatasourceDependencies(
            store=store,
            vector_db=MagicMock(),
            knowledge_index=knowledge_index.KnowledgeIndexDependencies(
                store=store,
                vector_db=MagicMock(),
                gitea_client=MagicMock(),
                logger=MagicMock(),
                tasks=KbDatasourceTaskRegistry(),
                inject_system_kb_embedding_profile=MagicMock(),
            ),
            mcp_datasources_enabled=mcp_datasources_enabled,
            validate_mcp_datasource=validate_mcp_datasource,
        ),
        require_datasource_owner=datasource_owner,
    )
    app = mount_router(
        router, factories={"datasources_dependencies_factory": lambda: dependencies}
    )
    response = TestClient(app).post(f"/api/datasources/{case.row['id']}/test")
    result: dict[str, Any] = {
        "status": response.status_code,
        "body": response.json(),
        "probe_calls": calls,
    }
    if case.pinned_defect:
        result["pinned_defect"] = case.pinned_defect
    return result


@pytest.fixture(scope="module")
def golden():
    golden = Golden("probe", CASES)
    yield golden
    golden.flush()


@pytest.mark.parametrize("case_id", list(CASES))
def test_connector_probe_matches_golden(case_id, golden, monkeypatch):
    golden.check(case_id, run_case(CASES[case_id], monkeypatch))


def test_every_type_is_probed():
    """Each of the 13 types has at least one Test case."""
    from shared.runtime.core.datasource_catalog import DATASOURCE_TYPE_IDS

    probed = {case.row["type"] for case in CASES.values()}
    assert probed == set(DATASOURCE_TYPE_IDS)


def test_golden_covers_every_case(golden):
    golden.assert_covers_cases()
