"""Golden pins: connector create, update and delete through the HTTP API.

Each case drives the real router and service (``POST``/``PUT``/``DELETE
/api/datasources``) against an in-memory store and records three things:

* the HTTP status and body, so every 400/403/409 detail is pinned verbatim;
* every store write and capability lookup with its arguments, which is the
  normalized connector the service hands to persistence (what a driver's
  ``validate`` must keep producing);
* the KB index side effects (mark pending, schedule a rebuild, fenced delete).

Gate order is part of the contract: some refusals fire before the caller is
authenticated. Cases with ``authenticated=False`` use a refusing approval gate
(401) to show which checks run first.

The store is a fake, so persistence-side refusals (a duplicate name, a stale
policy revision, a credential connector still attached) are injected with the
exception the real store raises; the case pins how the API maps them.

Regenerate: ``UPDATE_CONNECTOR_GOLDENS=1 python -m pytest
tests/test_connector_goldens_api.py`` (see ``tests/_connector_goldens.py``).
"""

from __future__ import annotations

import copy
from dataclasses import dataclass, field
from typing import Any, Literal
from unittest.mock import MagicMock

import asyncpg
import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from orchestrator.services.datasource_policy_errors import (
    DatasourcePolicyConflictError,
    DatasourcePolicyValidationError,
    DatasourceProjectAuthorizationError,
    DatasourceScopeAuthorizationError,
)
from shared.credential_connectors import CredentialConnectorAttachedError
from tests._connector_goldens import (
    DATASOURCE_ID,
    FIXED_TS,
    OTHER_PROJECT_ID,
    PROJECT_ID,
    SSH_PRIVATE_KEY,
    SSH_PUBLIC_KEY,
    UPDATE,
    USER,
    USER_ID,
    Golden,
    resolved_row,
)
from tests._mounted_router import mount_router

SCOPED_USER = {"id": USER_ID, "is_admin": False, "scopes": [f"project:{PROJECT_ID}"]}

#: Persistence refusals, keyed by name, raised with the real store's text.
STORE_FAILURES = {
    "unique": lambda: asyncpg.UniqueViolationError(
        'duplicate key value violates unique constraint "uq_datasource_name_type_owner"'
    ),
    "project_authorization": lambda: DatasourceProjectAuthorizationError(
        "Project owner authority is required for every added link"
    ),
    "policy_validation": lambda: DatasourcePolicyValidationError(
        "Project-scoped connectors require at least one project"
    ),
    "policy_conflict": lambda: DatasourcePolicyConflictError(
        "Connector policy changed; reload it and try again"
    ),
    "scope": lambda: DatasourceScopeAuthorizationError(
        "Connector mutation exceeds the caller's project scope"
    ),
    "credential_attached": lambda: CredentialConnectorAttachedError(
        "End the sessions and jobs using this credential connector before deleting it"
    ),
    "unexpected": lambda: RuntimeError("store exploded"),
}


@dataclass(frozen=True)
class ApiCase:
    op: Literal["create", "update", "delete"]
    body: dict[str, Any] | None = None
    #: The stored row the owner gate resolves (update/delete).
    existing: dict[str, Any] | None = None
    user: dict[str, Any] = field(default_factory=lambda: dict(USER))
    authenticated: bool = True
    mcp: bool = True
    stdio: bool = True
    can_publish: bool = True
    can_send: bool = False
    linked_project_ids: tuple[str, ...] = ()
    #: store method -> STORE_FAILURES key
    fail: dict[str, str] = field(default_factory=dict)
    pinned_defect: str | None = None


def _with(base: dict[str, Any], **over: Any) -> dict[str, Any]:
    out = copy.deepcopy(base)
    out.update(copy.deepcopy(over))
    return out


def _without(base: dict[str, Any], *keys: str) -> dict[str, Any]:
    out = copy.deepcopy(base)
    for key in keys:
        out.pop(key, None)
    return out


def _stored(kind: str, **over: Any) -> dict[str, Any]:
    """The stored row as the owner gate returns it (no project link)."""
    row = resolved_row(kind, **over)
    row.pop("project_read_only")
    return row


# =============================================================================
# Create bodies (raw, pre-normalization input)
# =============================================================================

GENERIC = {
    "name": "Billing API",
    "type": "generic",
    "description": "Billing REST API",
    "connection_url": "https://billing.example.test/api",
    "cli_hint": "curl $BILLING_URL",
    "credentials": {"env_vars": {"BILLING_TOKEN": "billing-secret"}},
}
CREDENTIALS = {
    "name": "Vendor login",
    "type": "credentials",
    "credentials": {
        "env_vars": {"VENDOR_USER": "alice", "VENDOR_PASSWORD": "vendor-secret"}
    },
}
GENERIC_FILE = {
    "name": "Service account",
    "type": "generic_file",
    "credentials": {
        "files": [
            {
                "contents": '{"type": "service_account"}',
                "target_path": "~/.config/gcloud/./sa.json",
                "env_var": "GOOGLE_APPLICATION_CREDENTIALS",
            },
            {
                "name": "ca.pem",
                "contents": "ca",
                "target_path": "/tmp/ca.pem",
                "mode": "0644",
                "env_var": "",
            },
        ]
    },
}
KUBECONFIG = {
    "name": "Staging Cluster",
    "type": "kubeconfig",
    "credentials": {"files": [{"contents": "apiVersion: v1\nkind: Config\n"}]},
}
SSH_KEY = {
    "name": "Deploy Key",
    "type": "ssh_key",
    "credentials": {"files": [{"contents": SSH_PRIVATE_KEY.rstrip("\n")}]},
}
REPOSITORY_TOKEN = {
    "name": "Widgets",
    "type": "repository",
    "connection_url": "https://github.com/acme/widgets.git",
    "default_branch": "main",
    "credentials": {"auth_method": "token", "token": "ghp_widgets-secret"},
}
REPOSITORY_SSH = {
    "name": "Gadgets",
    "type": "repository",
    "connection_url": "ssh://git@git.example.test:2222/acme/gadgets.git",
    "config": {"forge": "gitea"},
    "credentials": {
        "auth_method": "ssh",
        "ssh_key": "\r\n" + SSH_PRIVATE_KEY.replace("\n", "\r\n").rstrip(),
    },
}
POSTGRESQL = {
    "name": "Orders DB",
    "type": "postgresql",
    "connection_url": "postgresql://orders:pg-secret@db.example.test:5432/orders?sslmode=require&password=x",
}
NEO4J = {
    "name": "Supply Graph",
    "type": "neo4j",
    "connection_url": "bolt://neo4j.example.test:7687",
    "credentials": {"username": "graph", "password": "neo-secret"},
}
MONGODB = {
    "name": "Events",
    "type": "mongodb",
    "connection_url": "mongodb://events:mongo-secret@mongo.example.test:27017/events",
}
WEBDAV = {
    "name": "Team files",
    "type": "webdav",
    "connection_url": "https://cloud.example.test/remote.php/dav/files/alice",
    "credentials": {"username": "alice", "password": "dav-secret"},
}
EMAIL = {
    "name": "Support inbox",
    "type": "email",
    "config": {"folders": [" INBOX "]},
    "credentials": {
        "username": " support@example.test ",
        "password": "mail-secret",
        "imap": {"host": " imap.example.test "},
    },
}
EMAIL_SEND = _with(
    EMAIL,
    config={"access": "send", "folders": ["INBOX"], "recipient_allowlist": ["a@b"]},
    credentials={
        "username": "support@example.test",
        "password": "mail-secret",
        "imap": {"host": "imap.example.test", "security": "starttls"},
        "smtp": {"host": "smtp.example.test", "security": "starttls"},
    },
)
KB = {
    "name": "Handbook",
    "type": "kb",
    "connection_url": " https://git.example.test/acme/handbook.git ",
    "default_branch": "main",
    "config": {"root_path": "./vault//notes/", "forge": " Gitea "},
    "credentials": {"auth_method": "token", "token": "kb-secret"},
}
MCP_REMOTE = {
    "name": "Docs MCP",
    "type": "mcp",
    "connection_url": "https://mcp.example.test/mcp",
    "credentials": {
        "transport": "http",
        "auth": {"type": "bearer", "token": "mcp-secret"},
    },
}
MCP_STDIO = {
    "name": "Local MCP",
    "type": "mcp",
    "connection_url": "https://ignored.example.test/mcp",
    "credentials": {
        "transport": "STDIO",
        "command": "npx",
        "args": ["-y", "@acme/mcp"],
        "env": {"ACME_KEY": "stdio-secret"},
    },
}

_VALID_CREATE = {
    "generic": GENERIC,
    "credentials": CREDENTIALS,
    "generic_file": GENERIC_FILE,
    "kubeconfig": KUBECONFIG,
    "ssh_key": SSH_KEY,
    "repository": REPOSITORY_TOKEN,
    "postgresql": POSTGRESQL,
    "neo4j": NEO4J,
    "mongodb": MONGODB,
    "webdav": WEBDAV,
    "email": EMAIL,
    "kb": KB,
    "mcp": MCP_REMOTE,
}


def _create(body: dict[str, Any], **kwargs: Any) -> ApiCase:
    return ApiCase("create", body=body, **kwargs)


def _update(existing: dict[str, Any], body: dict[str, Any], **kwargs: Any) -> ApiCase:
    return ApiCase("update", body=body, existing=existing, **kwargs)


CASES: dict[str, ApiCase] = {
    # ---- cross-type create gates -------------------------------------------
    "create/unknown_type_before_auth": _create(
        {"name": "FTP", "type": "ftp"}, authenticated=False
    ),
    "create/job_id_refused_before_auth": _create(
        _with(GENERIC, job_id=DATASOURCE_ID), authenticated=False
    ),
    "create/publish_without_capability": _create(
        _with(POSTGRESQL, is_global=True), can_publish=False
    ),
    "create/scoped_token_outside_its_project": _create(GENERIC, user=SCOPED_USER),
    "create/scoped_token_in_its_project": _create(
        _with(GENERIC, scope_mode="projects", project_ids=[PROJECT_ID]),
        user=SCOPED_USER,
    ),
    "create/project_scope_links": _create(
        _with(
            GENERIC,
            scope_mode="projects",
            project_ids=[PROJECT_ID, OTHER_PROJECT_ID, PROJECT_ID],
            auto_attach=True,
        )
    ),
    "create/project_link_refused_by_store": _create(
        _with(GENERIC, scope_mode="projects", project_ids=[PROJECT_ID]),
        fail={"create_datasource": "project_authorization"},
    ),
    "create/policy_refused_by_store": _create(
        GENERIC, fail={"create_datasource": "policy_validation"}
    ),
    "create/unexpected_store_error": _create(
        GENERIC, fail={"create_datasource": "unexpected"}
    ),
    # ---- generic -------------------------------------------------------------
    "create/generic/valid": _create(GENERIC),
    "create/generic/public_defaults_read_only": _create(_with(GENERIC, is_global=True)),
    "create/generic/env_not_validated": _create(
        _with(GENERIC, credentials={"env_vars": {"1BAD": 3, "PATH": "/x"}}),
        pinned_defect=(
            "generic env vars are not validated at create; a bad set is only "
            "refused when it is installed at attach (L1 §2)"
        ),
    ),
    "create/generic/config_refused": _create(_with(GENERIC, config={"x": 1})),
    # ---- credentials ---------------------------------------------------------
    "create/credentials/valid": _create(CREDENTIALS),
    "create/credentials/missing_credentials": _create(
        _without(CREDENTIALS, "credentials")
    ),
    "create/credentials/empty_env": _create(
        _with(CREDENTIALS, credentials={"env_vars": {}})
    ),
    "create/credentials/env_not_an_object": _create(
        _with(CREDENTIALS, credentials={"env_vars": ["A"]})
    ),
    "create/credentials/invalid_name": _create(
        _with(CREDENTIALS, credentials={"env_vars": {"1KEY": "x"}})
    ),
    "create/credentials/reserved_name": _create(
        _with(CREDENTIALS, credentials={"env_vars": {"PATH": "x"}})
    ),
    "create/credentials/reserved_prefix": _create(
        _with(CREDENTIALS, credentials={"env_vars": {"SRW_TOKEN": "x"}})
    ),
    "create/credentials/non_string_value": _create(
        _with(CREDENTIALS, credentials={"env_vars": {"KEY": 3}})
    ),
    "create/credentials/published": _create(_with(CREDENTIALS, is_global=True)),
    "create/credentials/published_without_capability": _create(
        _with(CREDENTIALS, is_global=True), can_publish=False
    ),
    "create/credentials/validated_after_auth": _create(
        _with(CREDENTIALS, credentials={"env_vars": {}}), authenticated=False
    ),
    "create/credentials/config_refused": _create(_with(CREDENTIALS, config={"a": 1})),
    # ---- generic_file --------------------------------------------------------
    "create/generic_file/valid": _create(GENERIC_FILE),
    "create/generic_file/no_credentials": _create(
        _without(GENERIC_FILE, "credentials")
    ),
    "create/generic_file/no_files": _create(
        _with(GENERIC_FILE, credentials={"files": []})
    ),
    "create/generic_file/too_many_files": _create(
        _with(
            GENERIC_FILE,
            credentials={
                "files": [
                    {"contents": "x", "target_path": f"/tmp/f{index}"}
                    for index in range(6)
                ]
            },
        )
    ),
    "create/generic_file/entry_not_an_object": _create(
        _with(GENERIC_FILE, credentials={"files": ["x"]})
    ),
    "create/generic_file/missing_contents": _create(
        _with(GENERIC_FILE, credentials={"files": [{"target_path": "/tmp/x"}]})
    ),
    "create/generic_file/contents_too_large": _create(
        _with(
            GENERIC_FILE,
            credentials={
                "files": [{"contents": "x" * (64 * 1024 + 1), "target_path": "/tmp/x"}]
            },
        )
    ),
    "create/generic_file/missing_target_path": _create(
        _with(GENERIC_FILE, credentials={"files": [{"contents": "x"}]})
    ),
    "create/generic_file/relative_target_path": _create(
        _with(
            GENERIC_FILE,
            credentials={"files": [{"contents": "x", "target_path": "data/x"}]},
        )
    ),
    "create/generic_file/blocked_root": _create(
        _with(
            GENERIC_FILE,
            credentials={"files": [{"contents": "x", "target_path": "/etc/hosts"}]},
        )
    ),
    "create/generic_file/traversal_into_blocked_root": _create(
        _with(
            GENERIC_FILE,
            credentials={
                "files": [{"contents": "x", "target_path": "~/../../etc/passwd"}]
            },
        )
    ),
    "create/generic_file/outside_writable_roots": _create(
        _with(
            GENERIC_FILE,
            credentials={"files": [{"contents": "x", "target_path": "/opt/x"}]},
        )
    ),
    "create/generic_file/reserved_file": _create(
        _with(
            GENERIC_FILE,
            credentials={"files": [{"contents": "x", "target_path": "~/.bashrc"}]},
        )
    ),
    "create/generic_file/bad_mode": _create(
        _with(
            GENERIC_FILE,
            credentials={
                "files": [{"contents": "x", "target_path": "/tmp/x", "mode": "600"}]
            },
        )
    ),
    "create/generic_file/bad_env_var": _create(
        _with(
            GENERIC_FILE,
            credentials={
                "files": [
                    {"contents": "x", "target_path": "/tmp/x", "env_var": "BAD-NAME"}
                ]
            },
        )
    ),
    "create/generic_file/config_refused": _create(_with(GENERIC_FILE, config={"a": 1})),
    # ---- kubeconfig ----------------------------------------------------------
    "create/kubeconfig/valid_defaults": _create(KUBECONFIG),
    "create/kubeconfig/explicit_target": _create(
        _with(
            KUBECONFIG,
            credentials={
                "files": [
                    {
                        "name": "prod.yaml",
                        "contents": "apiVersion: v1\n",
                        "target_path": "/home/srw/.kube/prod.yaml",
                        "mode": "0640",
                        "env_var": "KUBECONFIG",
                    }
                ]
            },
        )
    ),
    "create/kubeconfig/two_files": _create(
        _with(
            KUBECONFIG,
            credentials={"files": [{"contents": "a"}, {"contents": "b"}]},
        )
    ),
    "create/kubeconfig/unnamed_slug": _create(_with(KUBECONFIG, name="!!!")),
    # ---- ssh_key -------------------------------------------------------------
    "create/ssh_key/private_only": _create(
        SSH_KEY,
        pinned_defect=(
            "C1 validates the key (it must parse, without a passphrase); the "
            "file contents are still stored as given (no newline normalisation)"
        ),
    ),
    "create/ssh_key/private_and_public": _create(
        _with(
            SSH_KEY,
            credentials={
                "files": [
                    {"contents": SSH_PRIVATE_KEY},
                    {"contents": "ssh-ed25519 AAAA golden@test"},
                ]
            },
        )
    ),
    "create/ssh_key/not_a_key_refused": _create(
        _with(SSH_KEY, credentials={"files": [{"contents": "not a key"}]}),
    ),
    "create/ssh_key/three_files": _create(
        _with(
            SSH_KEY,
            credentials={
                "files": [{"contents": "a"}, {"contents": "b"}, {"contents": "c"}]
            },
        )
    ),
    "create/ssh_key/top_level_ssh_key_is_validated": _create(
        _with(SSH_KEY, credentials={"ssh_key": "not a key", "files": []})
    ),
    # ---- repository ----------------------------------------------------------
    "create/repository/token_github_forge_inferred": _create(REPOSITORY_TOKEN),
    "create/repository/token_gitlab_forge_inferred": _create(
        _with(REPOSITORY_TOKEN, connection_url="https://www.gitlab.com/acme/w.git")
    ),
    "create/repository/explicit_forge_normalised": _create(
        _with(
            REPOSITORY_TOKEN,
            connection_url="https://git.example.test/acme/widgets.git",
            config={"forge": " GitLab ", "extra": "kept"},
        )
    ),
    "create/repository/self_hosted_without_forge": _create(
        _with(REPOSITORY_TOKEN, connection_url="https://git.example.test/acme/w.git")
    ),
    "create/repository/unsupported_forge": _create(
        _with(REPOSITORY_TOKEN, config={"forge": "bitbucket"})
    ),
    "create/repository/no_url_without_forge": _create(
        _without(REPOSITORY_TOKEN, "connection_url")
    ),
    "create/repository/credentials_not_validated": _create(
        _with(REPOSITORY_TOKEN, credentials={"read_only": True})
    ),
    "create/repository/ssh_key_normalised": _create(REPOSITORY_SSH),
    "create/repository/ssh_key_invalid": _create(
        _with(REPOSITORY_SSH, credentials={"auth_method": "ssh", "ssh_key": "nope"})
    ),
    "create/repository/scp_url_without_forge": _create(
        _with(
            REPOSITORY_SSH,
            connection_url="git@github.com:acme/widgets.git",
            config=None,
        ),
        pinned_defect=(
            "an scp-style URL has no parsable host, so github.com is not "
            "inferred and the create is refused"
        ),
    ),
    "create/repository/scp_url_with_forge": _create(
        _with(
            REPOSITORY_SSH,
            connection_url="git@github.com:acme/widgets.git",
            config={"forge": "github"},
        )
    ),
    # ---- managed databases ---------------------------------------------------
    "create/postgresql/valid_url_redacted_in_response": _create(POSTGRESQL),
    "create/postgresql/public_defaults_read_only": _create(
        _with(POSTGRESQL, is_global=True)
    ),
    "create/postgresql/public_explicit_read_write": _create(
        _with(POSTGRESQL, is_global=True, read_only=False)
    ),
    "create/postgresql/config_refused": _create(_with(POSTGRESQL, config={"a": 1})),
    "create/postgresql/no_url_accepted": _create(
        _without(POSTGRESQL, "connection_url")
    ),
    "create/neo4j/valid": _create(NEO4J),
    "create/neo4j/config_refused": _create(_with(NEO4J, config={"database": "x"})),
    "create/mongodb/valid": _create(MONGODB),
    "create/mongodb/config_refused": _create(_with(MONGODB, config={"a": 1})),
    "create/webdav/valid": _create(WEBDAV),
    "create/webdav/config_refused": _create(_with(WEBDAV, config={"a": 1})),
    # ---- email ---------------------------------------------------------------
    "create/email/draft_defaults": _create(EMAIL),
    "create/email/send_with_smtp": _create(EMAIL_SEND),
    "create/email/unattended_send_without_grant": _create(
        _with(
            EMAIL_SEND,
            config={"access": "send", "folders": ["INBOX"], "unattended_send": True},
        )
    ),
    "create/email/unattended_send_with_grant": _create(
        _with(
            EMAIL_SEND,
            config={"access": "send", "folders": ["INBOX"], "unattended_send": True},
        ),
        can_send=True,
    ),
    "create/email/send_without_folders": _create(
        _with(EMAIL_SEND, config={"access": "send"})
    ),
    "create/email/send_without_smtp": _create(
        _with(
            EMAIL_SEND,
            credentials=_without(EMAIL_SEND["credentials"], "smtp"),
        )
    ),
    "create/email/bad_access": _create(_with(EMAIL, config={"access": "admin"})),
    "create/email/unknown_config_field": _create(_with(EMAIL, config={"tier": "x"})),
    "create/email/bad_folders": _create(_with(EMAIL, config={"folders": "INBOX"})),
    "create/email/missing_credentials": _create(_without(EMAIL, "credentials")),
    "create/email/unknown_credential_field": _create(
        _with(EMAIL, credentials={**EMAIL["credentials"], "token": "x"})
    ),
    "create/email/missing_password": _create(
        _with(EMAIL, credentials=_without(EMAIL["credentials"], "password"))
    ),
    "create/email/bad_imap_block": _create(
        _with(EMAIL, credentials={**EMAIL["credentials"], "imap": {"port": 993}})
    ),
    "create/email/bad_imap_security": _create(
        _with(
            EMAIL,
            credentials={
                **EMAIL["credentials"],
                "imap": {"host": "imap.example.test", "security": "tls"},
            },
        )
    ),
    "create/email/bad_backend": _create(
        _with(EMAIL, credentials={**EMAIL["credentials"], "backend": "gmail"})
    ),
    "create/email/published": _create(_with(EMAIL, is_global=True)),
    # ---- kb ------------------------------------------------------------------
    "create/kb/valid_normalised": _create(KB),
    "create/kb/public_forced_read_only": _create(_with(KB, is_global=True)),
    "create/kb/read_write_refused": _create(_with(KB, read_only=False)),
    "create/kb/missing_url": _create(_without(KB, "connection_url")),
    "create/kb/url_with_credentials": _create(
        _with(KB, connection_url="https://user:pw@git.example.test/acme/h.git")
    ),
    "create/kb/local_url_refused": _create(_with(KB, connection_url="/srv/git/h.git")),
    "create/kb/untrusted_host": _create(
        _with(KB, connection_url="https://git.other.test/acme/handbook.git")
    ),
    "create/kb/unknown_config_field": _create(_with(KB, config={"branch": "x"})),
    "create/kb/root_path_escape": _create(_with(KB, config={"root_path": "../x"})),
    "create/kb/root_path_absolute": _create(_with(KB, config={"root_path": "/x"})),
    "create/kb/unsupported_forge": _create(_with(KB, config={"forge": "svn"})),
    "create/kb/token_over_http": _create(
        _with(KB, connection_url="http://git.example.test/acme/handbook.git")
    ),
    "create/kb/ssh_url_without_key": _create(
        _with(
            KB,
            connection_url="ssh://git@git.example.test/acme/handbook.git",
            credentials=None,
        )
    ),
    "create/kb/public_without_credentials": _create(_with(KB, credentials=None)),
    # ---- mcp -----------------------------------------------------------------
    "create/mcp/gate_off_before_auth": _create(
        MCP_REMOTE, mcp=False, authenticated=False
    ),
    "create/mcp/remote_bearer": _create(MCP_REMOTE),
    "create/mcp/remote_custom_headers": _create(
        _with(
            MCP_REMOTE,
            credentials={
                "transport": "sse",
                "auth": {"type": "headers", "headers": {"X-Api-Key": "k"}},
            },
        )
    ),
    "create/mcp/remote_no_auth": _create(
        _with(MCP_REMOTE, credentials=None, connection_url="http://mcp.example.test/")
    ),
    "create/mcp/remote_validated_before_auth": _create(
        _without(MCP_REMOTE, "connection_url"), authenticated=False
    ),
    "create/mcp/remote_non_http_url": _create(
        _with(MCP_REMOTE, connection_url="ftp://mcp.example.test/")
    ),
    "create/mcp/remote_url_with_userinfo": _create(
        _with(MCP_REMOTE, connection_url="https://u:p@mcp.example.test/mcp")
    ),
    "create/mcp/remote_unknown_credential_field": _create(
        _with(MCP_REMOTE, credentials={"transport": "http", "token": "x"})
    ),
    "create/mcp/remote_bad_transport": _create(
        _with(MCP_REMOTE, credentials={"transport": "websocket"})
    ),
    "create/mcp/remote_transport_not_a_string": _create(
        _with(MCP_REMOTE, credentials={"transport": 1})
    ),
    "create/mcp/remote_bearer_without_token": _create(
        _with(MCP_REMOTE, credentials={"auth": {"type": "bearer"}})
    ),
    "create/mcp/remote_bad_auth_type": _create(
        _with(MCP_REMOTE, credentials={"auth": {"type": "oauth"}})
    ),
    "create/mcp/remote_bad_header": _create(
        _with(
            MCP_REMOTE,
            credentials={"auth": {"type": "headers", "headers": {"X": "a\nb"}}},
        )
    ),
    "create/mcp/config_refused_after_auth": _create(_with(MCP_REMOTE, config={"a": 1})),
    "create/mcp/stdio_valid_url_dropped": _create(MCP_STDIO),
    "create/mcp/stdio_gate_off": _create(MCP_STDIO, stdio=False),
    "create/mcp/stdio_missing_command": _create(
        _with(MCP_STDIO, credentials={"transport": "stdio", "command": " "})
    ),
    "create/mcp/stdio_bad_args": _create(
        _with(
            MCP_STDIO, credentials={"transport": "stdio", "command": "x", "args": "a"}
        )
    ),
    "create/mcp/stdio_bad_env": _create(
        _with(
            MCP_STDIO,
            credentials={"transport": "stdio", "command": "x", "env": {"A=B": "c"}},
        )
    ),
    "create/mcp/stdio_unknown_field": _create(
        _with(MCP_STDIO, credentials={"transport": "stdio", "command": "x", "cwd": "/"})
    ),
}

# A duplicate name per type: the store's unique violation becomes a 409.
for _type, _body in _VALID_CREATE.items():
    CASES[f"create/{_type}/duplicate_name"] = _create(
        _body, fail={"create_datasource": "unique"}
    )

CASES.update(
    {
        # ---- cross-type update gates ---------------------------------------
        "update/publish_without_capability": _update(
            _stored("postgresql"), {"is_global": True}, can_publish=False
        ),
        "update/unpublish_without_capability": _update(
            _stored("postgresql", is_global=True, read_only=True),
            {"is_global": False},
            can_publish=False,
        ),
        "update/public_row_gets_read_only_default": _update(
            _stored("generic", is_global=True), {"description": "Edited"}
        ),
        "update/blank_body_writes_nothing": _update(_stored("generic"), {}),
        "update/scoped_token_cross_scope": _update(
            _stored("generic"),
            {"description": "Edited"},
            user=SCOPED_USER,
            linked_project_ids=(OTHER_PROJECT_ID,),
        ),
        "update/scoped_token_in_scope": _update(
            _stored("generic", scope_mode="projects"),
            {"description": "Edited"},
            user=SCOPED_USER,
            linked_project_ids=(PROJECT_ID,),
        ),
        "update/policy_change": _update(
            _stored("generic"),
            {
                "scope_mode": "projects",
                "project_ids": [PROJECT_ID, OTHER_PROJECT_ID],
                "auto_attach": True,
                "policy_revision": 1,
            },
            linked_project_ids=(PROJECT_ID,),
        ),
        "update/policy_conflict": _update(
            _stored("generic"),
            {"auto_attach": True, "policy_revision": 1},
            fail={"update_datasource_with_policy": "policy_conflict"},
        ),
        "update/scope_refused_by_store": _update(
            _stored("generic"),
            {"description": "Edited"},
            fail={"update_datasource": "scope"},
        ),
        # ---- generic ---------------------------------------------------------
        "update/generic/env_replaced_not_merged": _update(
            _stored("generic"),
            {"credentials": {"env_vars": {"OTHER": "x", "1BAD": "y"}}},
            pinned_defect=(
                "generic env vars are not validated on update either; the set "
                "is replaced, not merged"
            ),
        ),
        "update/generic/empty_credentials_keep_stored": _update(
            _stored("generic"), {"credentials": {}, "cli_hint": "new hint"}
        ),
        "update/generic/config_refused": _update(
            _stored("generic"), {"config": {"a": 1}}
        ),
        "update/generic/empty_config_ignored": _update(
            _stored("generic"), {"config": {}}
        ),
        # A stored type no driver serves gets only the type-free rules.
        "update/unknown_stored_type/config_refused": _update(
            _stored("generic", type="ftp"), {"config": {"a": 1}}
        ),
        "update/unknown_stored_type/credentials_stored": _update(
            _stored("generic", type="ftp"),
            {"credentials": {"token": "ftp-secret"}, "description": "Edited"},
        ),
        # ---- credentials -----------------------------------------------------
        "update/credentials/env_merged": _update(
            _stored("credentials"),
            {
                "credentials": {
                    "env_vars": {"VENDOR_PASSWORD": "rotated", "VENDOR_MFA": "123"}
                }
            },
        ),
        "update/credentials/reserved_name": _update(
            _stored("credentials"), {"credentials": {"env_vars": {"HOME": "x"}}}
        ),
        "update/credentials/empty_env_refused": _update(
            _stored("credentials"), {"credentials": {"env_vars": {}}}
        ),
        "update/credentials/blank_keeps_stored": _update(
            _stored("credentials"), {"credentials": {}, "name": "Vendor"}
        ),
        "update/credentials/publish_refused": _update(
            _stored("credentials"), {"is_global": True}
        ),
        # Authority before content: the scoped token may not widen the scope,
        # and that refusal wins over the invalid variable (D1a reordered
        # update validation after the policy checks, as on create).
        "update/credentials/scope_refused_before_validation": _update(
            _stored("credentials", scope_mode="projects"),
            {
                "scope_mode": "all",
                "policy_revision": 1,
                "credentials": {"env_vars": {"HOME": "x"}},
            },
            user=SCOPED_USER,
            linked_project_ids=(PROJECT_ID,),
        ),
        # ---- credential files ------------------------------------------------
        "update/generic_file/files_renormalised": _update(
            _stored("generic_file"),
            {"credentials": {"files": [{"contents": "y", "target_path": "~/y"}]}},
        ),
        "update/generic_file/invalid_files": _update(
            _stored("generic_file"),
            {"credentials": {"files": [{"contents": "y", "target_path": "/etc/y"}]}},
        ),
        "update/kubeconfig/default_target_follows_new_name": _update(
            _stored("kubeconfig"),
            {
                "name": "Prod Cluster",
                "credentials": {"files": [{"contents": "apiVersion: v1\n"}]},
            },
        ),
        "update/kubeconfig/rename_keeps_stored_files": _update(
            _stored("kubeconfig"), {"name": "Prod Cluster"}
        ),
        "update/kubeconfig/two_files": _update(
            _stored("kubeconfig"),
            {"credentials": {"files": [{"contents": "a"}, {"contents": "b"}]}},
        ),
        "update/ssh_key/replace_with_unparseable_key_refused": _update(
            _stored("ssh_key"),
            {
                "credentials": {
                    "files": [{"contents": "new private"}, {"contents": "new pub"}]
                }
            },
        ),
        "update/ssh_key/replace_with_pair": _update(
            _stored("ssh_key"),
            {
                "credentials": {
                    "files": [
                        {"contents": SSH_PRIVATE_KEY},
                        {"contents": SSH_PUBLIC_KEY},
                    ]
                }
            },
        ),
        "update/ssh_key/three_files": _update(
            _stored("ssh_key"),
            {
                "credentials": {
                    "files": [{"contents": "a"}, {"contents": "b"}, {"contents": "c"}]
                }
            },
        ),
        # ---- repository ------------------------------------------------------
        "update/repository/config_renormalised_against_stored_url": _update(
            _stored("repository_token"), {"config": {}}
        ),
        "update/repository/config_against_new_url": _update(
            _stored("repository_token"),
            {
                "connection_url": "https://gitlab.com/acme/widgets.git",
                "config": {"forge": ""},
            },
        ),
        "update/repository/self_hosted_config_without_forge": _update(
            _stored("repository_ssh"), {"config": {}}
        ),
        "update/repository/url_change_keeps_stored_forge": _update(
            _stored("repository_token"),
            {"connection_url": "https://git.example.test/acme/widgets.git"},
        ),
        "update/repository/token_replaces_whole_credentials": _update(
            _stored("repository_token"), {"credentials": {"token": "ghp_rotated"}}
        ),
        "update/repository/invalid_ssh_key": _update(
            _stored("repository_ssh"), {"credentials": {"ssh_key": "nope"}}
        ),
        # ---- managed databases -----------------------------------------------
        "update/postgresql/config_refused": _update(
            _stored("postgresql"), {"config": {"a": 1}}
        ),
        "update/postgresql/url_and_declared_read_only": _update(
            _stored("postgresql"),
            {
                "connection_url": "postgresql://orders:new@db2.example.test/orders",
                "read_only": True,
            },
        ),
        "update/neo4j/credentials_replaced": _update(
            _stored("neo4j"), {"credentials": {"password": "rotated"}}
        ),
        "update/mongodb/config_refused": _update(
            _stored("mongodb"), {"config": {"a": 1}}
        ),
        "update/webdav/credentials_replaced": _update(
            _stored("webdav"),
            {"credentials": {"username": "bob", "password": "rotated"}},
        ),
        # ---- email -----------------------------------------------------------
        "update/email/publish_refused": _update(_stored("email"), {"is_global": True}),
        "update/email/config_normalised": _update(
            _stored("email"), {"config": {"access": "read", "folders": [" INBOX "]}}
        ),
        "update/email/send_without_stored_smtp": _update(
            _stored(
                "email",
                credentials={
                    "backend": "imap_smtp",
                    "username": "support@example.test",
                    "password": "mail-secret",
                    "imap": {
                        "host": "imap.example.test",
                        "port": 993,
                        "security": "ssl",
                    },
                },
            ),
            {"config": {"access": "send", "folders": ["INBOX"]}},
        ),
        "update/email/unattended_send_without_grant": _update(
            _stored("email"),
            {
                "config": {
                    "access": "send",
                    "folders": ["INBOX"],
                    "unattended_send": True,
                }
            },
        ),
        "update/email/credentials_validated": _update(
            _stored("email"), {"credentials": {"username": "x", "password": "y"}}
        ),
        "update/email/credentials_normalised": _update(
            _stored("email"),
            {
                "credentials": {
                    "username": " new@example.test ",
                    "password": "rotated",
                    "imap": {"host": "imap2.example.test", "security": "starttls"},
                }
            },
        ),
        "update/email/stored_credentials_rechecked": _update(
            _stored("email", credentials={"username": "x"}), {"description": "Edited"}
        ),
        # ---- kb --------------------------------------------------------------
        "update/kb/read_write_refused": _update(_stored("kb"), {"read_only": False}),
        "update/kb/root_path_change_reindexes": _update(
            _stored("kb"), {"config": {"root_path": "notes"}}
        ),
        "update/kb/same_config_no_reindex": _update(
            _stored("kb"), {"config": {"root_path": "./vault/", "forge": "gitea"}}
        ),
        "update/kb/url_change_reindexes": _update(
            _stored("kb"),
            {"connection_url": "https://git.example.test/acme/handbook2.git"},
        ),
        "update/kb/branch_change_reindexes": _update(
            _stored("kb"), {"default_branch": "release"}
        ),
        "update/kb/token_over_http_refused": _update(
            _stored("kb"),
            {"connection_url": "http://git.example.test/acme/handbook.git"},
        ),
        "update/kb/unknown_config_field": _update(
            _stored("kb"), {"config": {"branch": "x"}}
        ),
        "update/kb/native_policy_change_refused": _update(
            _stored("kb_native"), {"auto_attach": True, "policy_revision": 1}
        ),
        "update/kb/native_config_keeps_marker": _update(
            _stored("kb_native"),
            {
                "config": {"root_path": "notes"},
                "connection_url": "https://git.example.test/x.git",
            },
        ),
        "update/kb/native_marker_refused_in_input": _update(
            _stored("kb_native"),
            {"config": {"root_path": "notes", "native_project_id": OTHER_PROJECT_ID}},
        ),
        # ---- mcp -------------------------------------------------------------
        "update/mcp/gate_off": _update(
            _stored("mcp_remote"), {"description": "Edited"}, mcp=False
        ),
        "update/mcp/remote_token_rotation": _update(
            _stored("mcp_remote"),
            {"credentials": {"auth": {"type": "bearer", "token": "rotated"}}},
        ),
        "update/mcp/remote_invalid_merged_shape": _update(
            _stored("mcp_remote"), {"credentials": {"auth": {"type": "bearer"}}}
        ),
        "update/mcp/remote_clear_url_refused": _update(
            _stored("mcp_remote"), {"connection_url": None}
        ),
        "update/mcp/switch_to_stdio_clears_url": _update(
            _stored("mcp_remote"),
            {"credentials": {"transport": "stdio", "command": "uvx", "args": ["x"]}},
        ),
        "update/mcp/stdio_row_locked_when_stdio_gate_off": _update(
            _stored("mcp_stdio"), {"description": "Edited"}, stdio=False
        ),
        "update/mcp/config_refused": _update(
            _stored("mcp_remote"), {"config": {"a": 1}}
        ),
        # ---- delete ----------------------------------------------------------
        "delete/postgresql": ApiCase("delete", existing=_stored("postgresql")),
        "delete/kb/external_through_index_fence": ApiCase(
            "delete", existing=_stored("kb")
        ),
        "delete/kb/native_refused": ApiCase("delete", existing=_stored("kb_native")),
        "delete/credentials/attached_refused": ApiCase(
            "delete",
            existing=_stored("credentials"),
            fail={"delete_datasource": "credential_attached"},
        ),
    }
)


# =============================================================================
# Harness
# =============================================================================


class GoldenStore:
    """In-memory datasource store that records every write and grant lookup.

    ``update_datasource`` applies the real SQL's rules: ``None`` leaves a
    column alone unless ``connection_url_set`` clears the URL.
    """

    _CONTENT = (
        "name",
        "description",
        "connection_url",
        "credentials",
        "cli_hint",
        "default_branch",
        "config",
        "is_global",
        "read_only",
    )

    def __init__(self, case: ApiCase, calls: list[dict[str, Any]]) -> None:
        self.case = case
        self.calls = calls
        self.row = copy.deepcopy(case.existing)

    def _record(self, call: str, /, *args: Any, **kwargs: Any) -> None:
        entry: dict[str, Any] = {"call": call}
        if args:
            entry["args"] = list(args)
        if kwargs:
            entry["kwargs"] = kwargs
        self.calls.append(copy.deepcopy(entry))
        failure = self.case.fail.get(call)
        if failure:
            raise STORE_FAILURES[failure]()

    async def user_can_publish_datasource(self, _user):
        self._record("user_can_publish_datasource")
        return self.case.can_publish

    async def user_can_autonomous_send(self, _user):
        self._record("user_can_autonomous_send")
        return self.case.can_send

    async def record_security_event(self, **kwargs):
        self._record("record_security_event", **kwargs)

    async def list_datasource_projects(self, _datasource_id):
        return list(self.case.linked_project_ids)

    async def get_datasource(self, _datasource_id):
        return copy.deepcopy(self.row)

    async def create_datasource(self, **kwargs):
        self._record("create_datasource", **kwargs)
        self.row = {
            "id": DATASOURCE_ID,
            "name": kwargs["name"],
            "description": kwargs["description"],
            "type": kwargs["ds_type"],
            "connection_url": kwargs["connection_url"],
            "credentials": kwargs["credentials"],
            "config": kwargs["config"] or {},
            "job_id": kwargs["job_id"],
            "cli_hint": kwargs["cli_hint"],
            "default_branch": kwargs["default_branch"],
            "created_by": kwargs["created_by"],
            "is_global": kwargs["is_global"],
            "read_only": kwargs["read_only"],
            "scope_mode": kwargs["scope_mode"],
            "auto_attach": kwargs["auto_attach"],
            "policy_revision": 1,
            "created_at": FIXED_TS,
            "updated_at": FIXED_TS,
        }
        return copy.deepcopy(self.row)

    def _apply(self, kwargs: dict[str, Any]) -> None:
        for column in self._CONTENT:
            value = kwargs.get(column)
            if value is not None or (
                column == "connection_url" and kwargs.get("connection_url_set")
            ):
                self.row[column] = copy.deepcopy(value)

    async def update_datasource(self, datasource_id, **kwargs):
        self._record("update_datasource", datasource_id, **kwargs)
        self._apply(kwargs)
        return True

    async def update_datasource_with_policy(self, datasource_id, **kwargs):
        self._record("update_datasource_with_policy", datasource_id, **kwargs)
        self._apply(kwargs)
        for column in ("scope_mode", "auto_attach"):
            if kwargs.get(column) is not None:
                self.row[column] = kwargs[column]
        self.row["policy_revision"] = self.row.get("policy_revision", 1) + 1
        project_ids = kwargs.get("project_ids")
        return {
            "project_ids": list(
                project_ids if project_ids is not None else self.case.linked_project_ids
            )
        }

    async def delete_datasource(self, datasource_id, **kwargs):
        self._record("delete_datasource", datasource_id, **kwargs)
        return True


def _summarise_request(value: Any) -> Any:
    """Keep the request readable in the golden: long strings become a length."""
    if isinstance(value, dict):
        return {key: _summarise_request(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_summarise_request(item) for item in value]
    if isinstance(value, str) and len(value) > 300:
        return f"<{len(value)} chars>"
    return value


def _client(case: ApiCase, calls: list[dict[str, Any]], monkeypatch) -> TestClient:
    from orchestrator.routers.datasources import DatasourcesDependencies, router
    from orchestrator.services import knowledge_index
    from orchestrator.services.datasource_config import validate_mcp_datasource
    from orchestrator.services.datasources import DatasourceDependencies
    from orchestrator.services.deployment_gates import mcp_stdio_enabled
    from orchestrator.services.connector_drivers import builtin_connector_drivers
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
    # The KB create/update validators only accept admin-trusted Git hosts.
    monkeypatch.setenv("KB_GIT_ALLOWED_HOSTS", "git.example.test")

    async def mark_pending(datasource_id, *, dependencies):
        calls.append(
            {
                "call": "knowledge_index.mark_kb_datasource_pending",
                "args": [datasource_id],
            }
        )

    def schedule_reindex(datasource_id, *, force_full, dependencies):
        calls.append(
            {
                "call": "knowledge_index.schedule_kb_datasource_reindex",
                "args": [datasource_id],
                "kwargs": {"force_full": force_full},
            }
        )

    async def fenced_delete(datasource_id, **kwargs):
        kwargs.pop("dependencies")
        calls.append(
            {
                "call": "knowledge_index.delete_kb_datasource_with_index",
                "args": [datasource_id],
                "kwargs": kwargs,
            }
        )
        return True

    monkeypatch.setattr(knowledge_index, "mark_kb_datasource_pending", mark_pending)
    monkeypatch.setattr(
        knowledge_index, "schedule_kb_datasource_reindex", schedule_reindex
    )
    monkeypatch.setattr(
        knowledge_index, "delete_kb_datasource_with_index", fenced_delete
    )

    store = GoldenStore(case, calls)
    caller = case.user

    async def approve(_request, _store):
        if not case.authenticated:
            raise HTTPException(status_code=401, detail="Not authenticated")
        return caller

    async def project_owner(_request, _store, project_id, **_kwargs):
        calls.append({"call": "require_project_owner", "args": [project_id]})
        return caller, {"id": project_id}

    async def datasource_owner(_request, _store, _datasource_id):
        return caller, copy.deepcopy(case.existing)

    operations = DatasourceDependencies(
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
        mcp_stdio_enabled=mcp_stdio_enabled,
        connector_drivers=builtin_connector_drivers(),
    )
    dependencies = DatasourcesDependencies(
        store=store,
        operations=operations,
        require_approved_user=approve,
        require_project_owner=project_owner,
        require_datasource_owner=datasource_owner,
    )
    app = mount_router(
        router, factories={"datasources_dependencies_factory": lambda: dependencies}
    )
    return TestClient(app)


def run_case(case: ApiCase, monkeypatch) -> dict[str, Any]:
    calls: list[dict[str, Any]] = []
    client = _client(case, calls, monkeypatch)
    if case.op == "create":
        method, path = "POST", "/api/datasources"
    else:
        path = f"/api/datasources/{case.existing['id']}"
        method = "PUT" if case.op == "update" else "DELETE"
    response = client.request(method, path, json=case.body)
    result: dict[str, Any] = {
        "request": _summarise_request(
            {"method": method, "path": path, "body": case.body}
        ),
        "status": response.status_code,
        "body": response.json(),
        "calls": calls,
    }
    if case.pinned_defect:
        result["pinned_defect"] = case.pinned_defect
    return result


@pytest.fixture(scope="module")
def golden():
    golden = Golden("api", CASES)
    yield golden
    golden.flush()


@pytest.mark.parametrize("case_id", list(CASES))
def test_connector_api_matches_golden(case_id, golden, monkeypatch):
    golden.check(case_id, run_case(CASES[case_id], monkeypatch))


def test_every_type_has_a_valid_create_and_a_duplicate(golden):
    """Each of the 13 types is created successfully and refused as a duplicate."""
    from shared.runtime.core.datasource_catalog import DATASOURCE_TYPE_IDS

    assert sorted(_VALID_CREATE) == sorted(DATASOURCE_TYPE_IDS)
    if UPDATE:
        return
    for type_id in DATASOURCE_TYPE_IDS:
        statuses = {
            entry["status"]
            for case, entry in golden.cases.items()
            if case.startswith(f"create/{type_id}/")
        }
        assert 200 in statuses, type_id
        assert golden.cases[f"create/{type_id}/duplicate_name"]["status"] == 409


def test_golden_covers_every_case(golden):
    golden.assert_covers_cases()
