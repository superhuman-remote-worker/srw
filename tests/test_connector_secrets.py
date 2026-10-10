"""Connector drivers D3b: a Connector's credentials in its resource secret.

* every built-in driver flattens its stored credentials into the slot keys
  the design names (``token``, ``env.<NAME>``, ``header.<Name>``,
  ``file.<n>``, a login's ``username`` and ``password``, ...) plus the full
  connection URL, and the secret rebuilds the stored object exactly, key
  order included, whatever its shape;
* the Connector document names the keys and never a value;
* each slot's ``update`` rule is what the datasource API stores (pinned
  against the API goldens, which this slice does not change);
* the delivery reader prefers the secret, and falls back to the row for an
  unauthorized id, a stale or pre-D3b resource and a missing secret; the
  payload it builds is byte for byte the row's;
* Test reads the secret before the driver probes;
* ``ManifestAuthority.connector_secret`` (decision 11): Catalog refused, a
  foreign secret by the ordinary scope rule, the Connector's own secret by
  the connector policy;
* no other resource may reference a Connector's secret, its owner included;
* the resource API refuses a ``connector-`` secret.

The decision-11 matrix on real rows, the write-through and the backfill are
in ``tests/test_connector_secrets_real_postgres.py``.
"""

from __future__ import annotations

import copy
import json
import logging
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest
from fastapi import HTTPException

from orchestrator.security.crypto import encrypt
from orchestrator.services import connector_secrets
from orchestrator.services.connector_drivers import builtin_connector_drivers
from orchestrator.services.connector_secrets import (
    CATALOG_SECRET_DETAIL,
    CONNECTOR_SECRET_DETAIL,
    FOREIGN_CONNECTOR_SECRET_DETAIL,
    ROW_KEY,
    SHAPE_KEY,
    URL_KEY,
    connector_secret_name,
    credential_refs,
    is_connector_secret_name,
    is_own_connector_secret,
    read_connector_credentials,
    secret_values,
    stored_credentials,
)
from orchestrator.services.datasource_policy import GENERIC_UNAVAILABLE_DETAIL
from orchestrator.services.manifest_authority import ManifestAuthority
from orchestrator.services.manifest_connectors import (
    connector_document,
    connector_scope,
    declared_config,
)
from shared.connectors.builtin import (
    DATASOURCE_SPECS,
    DEVELOPMENT_SPECS,
    LEASE_PROBE_SPEC,
)
from shared.connectors.env_names import connector_env_problem
from shared.manifests import preview_documents, validate_documents

REGISTRY = builtin_connector_drivers()
OWNER = "00000000-0000-4000-8000-0000000000a1"
STAMP = datetime(2026, 10, 8, 9, 0, tzinfo=timezone.utc)
KEY_PEM = "-----BEGIN OPENSSH PRIVATE KEY-----\ns3cret-key\n-----END OPENSSH PRIVATE KEY-----\n"


def _row(ds_type: str, index: int = 1, **over: Any) -> dict[str, Any]:
    row = {
        "id": UUID(f"00000000-0000-4000-8000-{index:012x}"),
        "name": f"{ds_type} connector",
        "description": None,
        "type": ds_type,
        "connection_url": None,
        "credentials": {},
        "config": {},
        "cli_hint": None,
        "default_branch": None,
        "created_by": OWNER,
        "is_global": False,
        "read_only": None,
        "job_id": None,
        "managed_key": None,
        "updated_at": STAMP,
    }
    row.update(over)
    return row


def _files(*contents: str) -> dict[str, Any]:
    return {
        "files": [
            {
                "name": f"file-{index}",
                "contents": value,
                "target_path": f"~/.config/tool/file-{index}",
                "mode": "0600",
                "env_var": f"TOOL_FILE_{index}",
            }
            for index, value in enumerate(contents)
        ]
    }


#: (label, row, the secret's keys, values that must never reach a document)
SLOT_CASES = [
    (
        "generic/env",
        _row(
            "generic",
            connection_url="https://api.example.test/v1?key=s3cret-query",
            credentials={"env_vars": {"API_KEY": "s3cret-env", "REGION": "eu"}},
        ),
        {"env.API_KEY", "env.REGION", SHAPE_KEY, URL_KEY},
        ("s3cret-env", "s3cret-query"),
    ),
    (
        "generic/other_top_level_fields",
        _row(
            "generic",
            credentials={"api_key": "s3cret-key", "retries": 3, "nested": {"a": 1}},
        ),
        {"api_key", SHAPE_KEY},
        ("s3cret-key",),
    ),
    (
        "credentials/env",
        _row(
            "credentials",
            credentials={
                "env_vars": {"VENDOR_USER": "alice", "VENDOR_PASSWORD": "s3cret-pw"}
            },
        ),
        {"env.VENDOR_USER", "env.VENDOR_PASSWORD", SHAPE_KEY},
        ("s3cret-pw",),
    ),
    (
        "repository/token",
        _row(
            "repository",
            connection_url="https://bot:s3cret-url@git.example.test/acme/w.git",
            credentials={"auth_method": "token", "token": "s3cret-token"},
        ),
        {"token", SHAPE_KEY, URL_KEY},
        ("s3cret-token", "s3cret-url"),
    ),
    (
        "repository/ssh",
        _row(
            "repository",
            connection_url="git@git.example.test:acme/w.git",
            credentials={"auth_method": "ssh", "ssh_key": KEY_PEM},
        ),
        {"ssh_key", SHAPE_KEY, URL_KEY},
        ("s3cret-key",),
    ),
    (
        "repository/no_auth_flag",
        # The cockpit stores this for a repository created without a token.
        _row(
            "repository",
            connection_url="https://git.example.test/acme/w.git",
            credentials={"read_only": True},
        ),
        {SHAPE_KEY, URL_KEY},
        (),
    ),
    (
        "kb/token",
        _row(
            "kb",
            connection_url="https://git.example.test/acme/vault.git",
            credentials={"auth_method": "token", "token": "s3cret-token"},
        ),
        {"token", SHAPE_KEY, URL_KEY},
        ("s3cret-token",),
    ),
    (
        "postgresql/password_in_url",
        _row(
            "postgresql",
            connection_url="postgresql://app:s3cret-pass@db.example.test:5432/app",
        ),
        {URL_KEY},
        ("s3cret-pass",),
    ),
    (
        "mongodb/password_in_url",
        _row(
            "mongodb",
            connection_url="mongodb://app:s3cret-pass@a.example.test:27017,b:27017/x",
        ),
        {URL_KEY},
        ("s3cret-pass",),
    ),
    (
        "neo4j/login",
        _row(
            "neo4j",
            connection_url="bolt://graph.example.test:7687",
            credentials={"username": "s3cret-user", "password": "s3cret-pass"},
        ),
        {"username", "password", SHAPE_KEY, URL_KEY},
        ("s3cret-user", "s3cret-pass"),
    ),
    (
        "webdav/login",
        _row(
            "webdav",
            connection_url="https://dav.example.test/remote.php/dav",
            credentials={"username": "s3cret-user", "password": "s3cret-pass"},
        ),
        {"username", "password", SHAPE_KEY, URL_KEY},
        ("s3cret-user", "s3cret-pass"),
    ),
    (
        "email/mailbox",
        _row(
            "email",
            config={"access": "draft"},
            credentials={
                "backend": "imap_smtp",
                "username": "support@example.test",
                "password": "s3cret-pass",
                "imap": {"host": "imap.example.test", "port": 993, "security": "ssl"},
                "smtp": {"host": "smtp.example.test", "port": 465, "security": "ssl"},
            },
        ),
        {"password", SHAPE_KEY},
        ("s3cret-pass",),
    ),
    (
        "mcp/stdio",
        _row(
            "mcp",
            credentials={
                "transport": "stdio",
                "command": "s3cret-cmd",
                "args": ["--key", "s3cret-arg"],
                "env": {"TOKEN": "s3cret-env"},
            },
        ),
        {"command", "arg.0", "arg.1", "env.TOKEN", SHAPE_KEY},
        ("s3cret-cmd", "s3cret-arg", "s3cret-env"),
    ),
    (
        "mcp/stdio_empty_args_and_env",
        _row(
            "mcp",
            credentials={"transport": "stdio", "command": "uvx", "args": [], "env": {}},
        ),
        {"command", SHAPE_KEY},
        (),
    ),
    (
        "mcp/remote_bearer",
        _row(
            "mcp",
            connection_url="https://mcp.example.test/api/mcp/s/s3cret-path/mcp",
            credentials={
                "transport": "http",
                "auth": {"type": "bearer", "token": "s3cret-token"},
            },
        ),
        {"token", SHAPE_KEY, URL_KEY},
        ("s3cret-token", "s3cret-path"),
    ),
    (
        "mcp/remote_headers",
        _row(
            "mcp",
            connection_url="https://mcp.example.test/mcp",
            credentials={
                "transport": "sse",
                "auth": {"type": "headers", "headers": {"X-Api-Key": "s3cret-hdr"}},
            },
        ),
        {"header.X-Api-Key", SHAPE_KEY, URL_KEY},
        ("s3cret-hdr",),
    ),
    (
        "mcp/remote_no_auth",
        _row(
            "mcp",
            connection_url="https://mcp.example.test/mcp",
            credentials={"transport": "http"},
        ),
        {SHAPE_KEY, URL_KEY},
        (),
    ),
    (
        "kubeconfig/file",
        _row("kubeconfig", credentials=_files("s3cret-kubeconfig")),
        {"file.0", SHAPE_KEY},
        ("s3cret-kubeconfig",),
    ),
    (
        "generic_file/two_files",
        _row("generic_file", credentials=_files("s3cret-one", "s3cret-two")),
        {"file.0", "file.1", SHAPE_KEY},
        ("s3cret-one", "s3cret-two"),
    ),
    (
        "ssh_key/pair",
        _row(
            "ssh_key",
            config={"host": "ssh.example.test", "user": "deploy"},
            credentials=_files(KEY_PEM, "ssh-ed25519 AAAA s3cret-comment"),
        ),
        {"file.0", "file.1", SHAPE_KEY},
        ("s3cret-key", "s3cret-comment"),
    ),
    (
        "lease_probe/secret",
        _row("lease_probe", credentials={"secret": "s3cret-upstream"}),
        {"secret", SHAPE_KEY},
        ("s3cret-upstream",),
    ),
    (
        "kb/native_nothing_secret",
        _row("kb", config={"root_path": "knowledge"}, created_by=None),
        set(),
        (),
    ),
]


PROBE_REGISTRY = builtin_connector_drivers(lease_probe=True)


def _driver(row):
    return PROBE_REGISTRY.for_type(row["type"])


class TestTheSlotKeys:
    def test_every_stored_type_has_a_case(self):
        covered = {row["type"] for _label, row, _keys, _secrets in SLOT_CASES}
        assert covered == {spec.legacy_type for spec in DATASOURCE_SPECS} | {
            LEASE_PROBE_SPEC.legacy_type
        }

    @pytest.mark.parametrize(
        ("label", "row", "keys", "secrets"),
        SLOT_CASES,
        ids=[case[0] for case in SLOT_CASES],
    )
    def test_each_driver_flattens_its_slots(self, label, row, keys, secrets):
        values = secret_values(_driver(row), row)
        # Plus the digest of the row it was written from, when there is one.
        assert set(values) == (keys | {ROW_KEY} if keys else set())
        assert all(isinstance(value, str) for value in values.values())
        if URL_KEY in keys:
            # The full URL, userinfo, path and query included.
            assert values[URL_KEY] == row["connection_url"]
        # The rest of the object never carries a value the slots took out.
        shape = values.get(SHAPE_KEY, "")
        assert not [secret for secret in secrets if secret in shape]

    @pytest.mark.parametrize(
        ("label", "row", "keys", "secrets"),
        SLOT_CASES,
        ids=[case[0] for case in SLOT_CASES],
    )
    def test_the_secret_rebuilds_the_stored_row_exactly(
        self, label, row, keys, secrets
    ):
        values = secret_values(_driver(row), row)
        # Through the store's encryption, as a delivery reads it back.
        values = json.loads(json.dumps(values))
        credentials, url = stored_credentials(values)
        assert credentials == row["credentials"]
        assert url == row["connection_url"]
        # Key order too: the agent's payload bytes do not change.
        assert json.dumps(credentials) == json.dumps(row["credentials"])

    @pytest.mark.parametrize(
        ("label", "row", "keys", "secrets"),
        SLOT_CASES,
        ids=[case[0] for case in SLOT_CASES],
    )
    def test_the_document_names_the_keys_and_never_a_value(
        self, label, row, keys, secrets
    ):
        driver = _driver(row)
        name = driver.resource_driver(row["credentials"])
        values = secret_values(driver, row)
        document = connector_document(
            row,
            scope=connector_scope(row, ["00000000-0000-4000-8000-0000000000b1"]),
            driver=name,
            credential_config=driver.credential_config(row["credentials"]),
            declared=declared_config(PROBE_REGISTRY.get(name).spec),
            credentials=credential_refs(connector_secret_name(row["id"]), values),
        )
        validate_documents([document])
        resolved = preview_documents([document])["resolved"][0]
        refs = document["spec"]["credentials"]
        assert set(refs) == keys
        assert {ref["secretRef"]["key"] for ref in refs.values()} == keys
        assert {ref["secretRef"]["name"] for ref in refs.values()} <= {
            "connector-" + row["id"].hex
        }
        text = json.dumps([document, resolved])
        assert not [secret for secret in secrets if secret in text]


class TestOddShapes:
    @pytest.mark.parametrize(
        "credentials",
        [
            # A credential field named like a reserved key stays in the shape.
            {
                "url": "s3cret-a",
                "shape": "s3cret-b",
                "row": "s3cret-c",
                "env_vars": {"A": "1"},
            },
            # Non-string values a slot names stay where they are.
            {"env_vars": {"A": 1, "B": None, "C": "c"}},
            {"env_vars": {}},
            # A top-level name that collides with a slot key.
            {"env.A": "top", "env_vars": {"A": "slot"}},
            # A legacy object that is not an object at all.
            ["s3cret-a", "s3cret-b"],
            {"deep": {"list": [{"x": "y"}, 2, None]}, "token": "t"},
        ],
    )
    def test_any_stored_shape_round_trips(self, credentials):
        row = _row("generic", credentials=copy.deepcopy(credentials))
        values = json.loads(json.dumps(secret_values(_driver(row), row)))
        rebuilt, url = stored_credentials(values)
        assert json.dumps(rebuilt) == json.dumps(credentials)
        assert url is None

    def test_a_key_two_leaves_share_keeps_the_first_and_shapes_the_second(self):
        """No built-in driver names one key twice (prefixed keys have a dot,
        field names cannot), but a driver that did loses nothing."""

        class Colliding:
            def secret_leaves(self, credentials):
                return [(("a",), "k"), (("b",), "k")]

        row = _row("generic", credentials={"a": "first", "b": "second"})
        values = secret_values(Colliding(), row)
        assert values["k"] == "first"
        assert json.loads(values[SHAPE_KEY])["credentials"] == {
            "a": None,
            "b": "second",
        }
        assert stored_credentials(values)[0] == {"a": "first", "b": "second"}

    def test_the_shape_holds_a_null_where_each_secret_goes_back(self):
        row = _row("neo4j", credentials={"username": "u", "password": "p", "n": 1})
        shape = json.loads(secret_values(_driver(row), row)[SHAPE_KEY])
        assert shape == {
            "credentials": {"username": None, "password": None, "n": 1},
            "secrets": [[["username"], "username"], [["password"], "password"]],
        }

    def test_an_empty_url_is_kept_as_stored(self):
        row = _row("generic", connection_url="")
        assert stored_credentials(secret_values(_driver(row), row)) == ({}, "")

    def test_nothing_secret_is_no_secret(self):
        row = _row("generic")
        assert secret_values(_driver(row), row) == {}
        assert stored_credentials({}) == ({}, None)

    @pytest.mark.parametrize(
        ("ds_type", "credentials", "keys"),
        [
            # A generic row's names are not validated on write: a pasted line,
            # a URL or a token with dashes never becomes a key name.
            (
                "generic",
                {
                    "env_vars": {
                        "API_KEY": "v1",
                        "API_KEY=s3cret-pasted": "v2",
                        "https://s3cret-host/x": "v3",
                        "sk-s3cret-dashed": "v4",
                        "X" * 129: "v5",
                    },
                    "s3cret field": "v6",
                    "api_token": "v7",
                },
                {"env.API_KEY", "api_token", SHAPE_KEY},
            ),
            (
                "mcp",
                {
                    "transport": "stdio",
                    "command": "npx",
                    "env": {"TOKEN": "v1", "s3cret name": "v2"},
                },
                {"command", "env.TOKEN", SHAPE_KEY},
            ),
        ],
        ids=["generic", "mcp_stdio"],
    )
    def test_only_environment_names_become_key_names(self, ds_type, credentials, keys):
        row = _row(ds_type, credentials=copy.deepcopy(credentials))
        values = secret_values(_driver(row), row)
        assert set(values) == keys | {ROW_KEY}
        assert "s3cret" not in " ".join(values)
        rebuilt, _url = stored_credentials(json.loads(json.dumps(values)))
        assert json.dumps(rebuilt) == json.dumps(credentials)


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["url", "shape"])
@pytest.mark.parametrize("ds_type", ["generic", "neo4j", "repository"])
async def test_new_credentials_cannot_use_a_reserved_top_level_field(ds_type, field):
    from orchestrator.services.connector_drivers.base import (
        RESERVED_FIELDS_DETAIL,
        ConnectorDraft,
    )

    driver = REGISTRY.for_type(ds_type)
    draft = ConnectorDraft(
        name="x",
        connection_url=None,
        credentials={field: "v", "token": "t"},
        config=None,
        read_only=None,
        is_global=None,
        default_branch=None,
        supplied=frozenset({"credentials"}),
    )
    for existing in (None, _row(ds_type)):
        with pytest.raises(HTTPException) as refused:
            driver.stored_credentials(draft, existing)
        assert (refused.value.status_code, refused.value.detail) == (
            400,
            RESERVED_FIELDS_DETAIL,
        )


def test_secret_names():
    uid = UUID("12345678-1234-4234-8234-123456789abc")
    assert connector_secret_name(uid) == "connector-12345678123442348234123456789abc"
    assert connector_secret_name(str(uid)) == connector_secret_name(uid)
    assert is_connector_secret_name(connector_secret_name(uid))
    assert not is_connector_secret_name("connector-123")
    assert not is_connector_secret_name("my-connector-secret")
    assert not is_connector_secret_name(None)


# =============================================================================
# The slots' update rules are what the datasource API stores
# =============================================================================


def test_every_built_in_slot_replaces_except_the_credentials_merge():
    rules = {
        (spec.name, slot.name): slot.update
        for spec in (*DATASOURCE_SPECS, *DEVELOPMENT_SPECS)
        for slot in spec.credential_slots
    }
    assert rules.pop(("srw.credentials/v1", "env_vars")) == "merge"
    assert set(rules.values()) == {"replace"}


def _leaf_values(driver, credentials) -> set[str]:
    if not isinstance(credentials, dict):
        return set()
    shape = json.loads(json.dumps(credentials))
    found = set()
    for path, _key in driver.secret_leaves(shape):
        value = shape
        for step in path:
            value = value[step]
        found.add(value)
    return found


def _api_update_cases():
    from tests._connector_goldens import GOLDEN_DIR
    from tests.test_connector_goldens_api import CASES

    golden = json.loads((GOLDEN_DIR / "api.json").read_text())
    for case_id, case in CASES.items():
        if case.op != "update" or "credentials" not in (case.body or {}):
            continue
        if PROBE_REGISTRY.for_type(case.existing["type"]) is None:
            # A stored type no driver serves never has a Connector.
            continue
        entry = golden[case_id]
        calls = [
            call
            for call in entry.get("calls", [])
            if call["call"] in ("update_datasource", "update_datasource_with_policy")
        ]
        if entry["status"] != 200 or not calls:
            continue
        yield case_id, case, calls[-1]["kwargs"]["credentials"]


API_UPDATE_CASES = list(_api_update_cases())


@pytest.mark.parametrize(
    ("case_id", "case", "stored"),
    API_UPDATE_CASES,
    ids=[case[0] for case in API_UPDATE_CASES],
)
def test_the_slot_rules_describe_what_the_api_stores(case_id, case, stored):
    """Each recorded update against the rule its driver's slots declare.

    A blank edit stores nothing (the row keeps its credentials, and the
    secret is written from the row); ``merge`` keeps what the edit did not
    name; ``replace`` keeps nothing the edit did not send.
    """
    existing = case.existing
    sent = case.body["credentials"]
    driver = PROBE_REGISTRY.for_type(existing["type"])
    if not sent:
        assert stored is None
        return
    assert stored is not None
    rules = {slot.update for slot in driver.spec.credential_slots}
    if "merge" in rules:
        # An edit also drops a stored name no connector may set (decision 36).
        previous = {
            name: value
            for name, value in existing["credentials"]["env_vars"].items()
            if connector_env_problem(name) is None
        }
        assert stored == {"env_vars": {**previous, **sent["env_vars"]}}
        return
    before = _leaf_values(driver, existing["credentials"])
    sent_values = _leaf_values(driver, sent)
    kept = {value for value in before - sent_values if value in json.dumps(stored)}
    assert kept == set(), case_id


def test_the_cases_cover_both_rules_and_a_blank_edit():
    rules = {
        case.existing["type"]: stored
        for _id, case, stored in API_UPDATE_CASES
        if case.body["credentials"]
    }
    assert "credentials" in rules and "neo4j" in rules and "repository" in rules
    assert any(not case.body["credentials"] for _id, case, _ in API_UPDATE_CASES)


# =============================================================================
# The delivery reader
# =============================================================================


def _stored_row(**over):
    row = _row(
        "neo4j",
        connection_url="bolt://graph.example.test:7687",
        credentials={"username": "graph", "password": "row-pass"},
    )
    row.update(over)
    return row


def _marked(row) -> dict[str, str]:
    """The row's secret with a different password and URL, as the write-through
    would not write it but with the row's digest kept: what a delivery reads
    proves it read the secret."""
    values = secret_values(_driver(row), row)
    values["password"] = "secret-pass"
    values[URL_KEY] = "bolt://other.example.test:7687"
    return values


def _record(row, *, values=None, refs=None, stamp=STAMP, scope="Account"):
    values = _marked(row) if values is None else values
    name = connector_secret_name(row["id"])
    return {
        "id": row["id"],
        "scope_kind": scope,
        "scope_name": OWNER,
        "linked_updated_at": stamp,
        "refs": json.dumps(credential_refs(name, values) if refs is None else refs),
        "ciphertext": encrypt(json.dumps(values)) if values else None,
        "keys": sorted(values),
    }


def _store(*records):
    return SimpleNamespace(
        store=SimpleNamespace(fetch=AsyncMock(return_value=list(records)))
    )


class TestTheReader:
    @pytest.mark.asyncio
    async def test_an_authorized_row_reads_its_connector_secret(self):
        row = _stored_row()
        dependencies = _store(_record(row))
        rows = [row]
        await read_connector_credentials(
            rows, authorized=[str(row["id"])], dependencies=dependencies
        )
        assert row["credentials"] == {"username": "graph", "password": "secret-pass"}
        assert row["connection_url"] == "bolt://other.example.test:7687"
        query, ids = dependencies.store.fetch.await_args.args
        assert ids == [row["id"]]
        # The secret joins its resource in the resource's own scope only.
        assert "s.scope_kind = r.scope_kind AND s.scope_name = r.scope_name" in query

    @pytest.mark.asyncio
    async def test_an_unauthorized_row_is_never_read(self):
        row = _stored_row()
        dependencies = _store(_record(row))
        await read_connector_credentials(
            [row],
            authorized=["00000000-0000-4000-8000-00000000ffff"],
            dependencies=dependencies,
        )
        dependencies.store.fetch.assert_not_awaited()
        assert row["credentials"]["password"] == "row-pass"

    @pytest.mark.asyncio
    async def test_a_row_read_without_its_stamp_keeps_its_credentials(self):
        row = _stored_row(updated_at=None)
        dependencies = _store()
        await read_connector_credentials(
            [row], authorized=[str(row["id"])], dependencies=dependencies
        )
        dependencies.store.fetch.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "change",
        [
            # Another version of the row wrote the resource.
            {"linked_updated_at": datetime(2026, 1, 1, tzinfo=timezone.utc)},
            # Written before D3b, or by an orchestrator without it.
            {"refs": None},
            # A reference to someone else's secret.
            {
                "refs": json.dumps(
                    {"password": {"secretRef": {"name": "other", "key": "password"}}}
                )
            },
            # A Catalog resource never lends a secret.
            {"scope_kind": "Catalog"},
        ],
        ids=["stale", "pre_d3b", "foreign_ref", "catalog"],
    )
    async def test_the_row_is_the_fallback(self, change):
        row = _stored_row()
        record = _record(row)
        record.update(change)
        await read_connector_credentials(
            [row], authorized=[str(row["id"])], dependencies=_store(record)
        )
        assert row["credentials"]["password"] == "row-pass"
        assert row["connection_url"] == "bolt://graph.example.test:7687"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "values",
        [
            # Written by hand: no digest of the row it stands for.
            lambda row: {
                key: value for key, value in _marked(row).items() if key != ROW_KEY
            },
            # Written from another row (a digest that is not this row's).
            lambda row: {**_marked(row), ROW_KEY: "0" * 64},
            # A key its resource names is missing.
            lambda row: {
                key: value for key, value in _marked(row).items() if key != "password"
            },
        ],
        ids=["by_hand", "another_row", "missing_key"],
    )
    async def test_a_secret_not_written_from_this_row_is_not_delivered(self, values):
        row = _stored_row()
        name = connector_secret_name(row["id"])
        record = _record(
            row,
            values=values(row),
            refs=credential_refs(name, secret_values(_driver(row), row)),
        )
        await read_connector_credentials(
            [row], authorized=[str(row["id"])], dependencies=_store(record)
        )
        assert row["credentials"]["password"] == "row-pass"

    @pytest.mark.asyncio
    async def test_a_secret_that_does_not_rebuild_falls_back_to_the_row(self, caplog):
        row = _stored_row()
        values = _marked(row)
        shape = json.loads(values[SHAPE_KEY])
        # The shape puts back a key the secret does not hold.
        shape["secrets"].append([["extra"], "token"])
        values[SHAPE_KEY] = json.dumps(shape)
        with caplog.at_level(logging.WARNING):
            await read_connector_credentials(
                [row],
                authorized=[str(row["id"])],
                dependencies=_store(_record(row, values=values)),
            )
        assert row["credentials"]["password"] == "row-pass"
        assert "does not rebuild" in caplog.text and "row-pass" not in caplog.text

    @pytest.mark.asyncio
    async def test_a_missing_secret_falls_back_and_says_so_without_a_value(
        self, caplog
    ):
        row = _stored_row()
        record = _record(row)
        record["ciphertext"] = None
        with caplog.at_level(logging.WARNING):
            await read_connector_credentials(
                [row], authorized=[str(row["id"])], dependencies=_store(record)
            )
        assert row["credentials"]["password"] == "row-pass"
        assert str(row["id"]) in caplog.text
        assert "row-pass" not in caplog.text

    @pytest.mark.asyncio
    async def test_a_connector_with_nothing_secret_reads_as_empty(self):
        # Its resource says so (``credentials: {}``) and it has no secret row.
        row = _stored_row(credentials={}, connection_url=None)
        record = _record(row, values={})
        assert record["ciphertext"] is None and json.loads(record["refs"]) == {}
        await read_connector_credentials(
            [row], authorized=[str(row["id"])], dependencies=_store(record)
        )
        assert row["credentials"] == {} and row["connection_url"] is None

    @pytest.mark.asyncio
    async def test_nothing_secret_is_believed_only_of_a_row_with_nothing(self):
        row = _stored_row()
        record = _record(row, values={})
        await read_connector_credentials(
            [row], authorized=[str(row["id"])], dependencies=_store(record)
        )
        assert row["credentials"]["password"] == "row-pass"

    def test_the_payload_of_every_golden_row_is_byte_identical(self):
        """Every kind the payload goldens pin, through its secret."""
        from orchestrator.services import agent_datasource_payload as payload
        from tests._connector_goldens import all_rows, wire_bytes

        dependencies = payload.DatasourcePayloadDependencies(
            logger=MagicMock(),
            mcp_datasources_enabled=lambda: True,
            connector_drivers=REGISTRY,
            workspace_ssh_known_hosts=lambda: "",
        )
        for read_only in (False, True):
            rows = all_rows(project_read_only=read_only)
            hydrated = copy.deepcopy(rows)
            for row in hydrated:
                values = json.loads(json.dumps(secret_values(_driver(row), row)))
                row["credentials"], row["connection_url"] = stored_credentials(values)
            assert wire_bytes(
                payload.build_datasources_payload(hydrated, dependencies=dependencies)
            ) == wire_bytes(
                payload.build_datasources_payload(rows, dependencies=dependencies)
            )


@pytest.mark.asyncio
async def test_test_connection_probes_with_what_the_secret_holds():
    from orchestrator.services.datasources import (
        DatasourceDependencies,
        test_datasource,
    )

    row = _stored_row()
    seen = {}

    class Driver:
        spec = SimpleNamespace(deployment_gate=None)

        def require_enabled(self, gates):
            pass

        async def check(self, ds, credentials, *, ctx):
            seen["credentials"] = credentials
            return {"status": "ok", "message": "Connected"}

    async def connector_credentials(rows, *, authorized):
        seen["authorized"] = list(authorized)
        rows[0]["credentials"] = {"username": "graph", "password": "secret-pass"}

    async def owned():
        return {"id": OWNER}, row

    result = await test_datasource(
        resolve_datasource=owned,
        dependencies=DatasourceDependencies(
            store=MagicMock(),
            vector_db=MagicMock(),
            knowledge_index=MagicMock(),
            mcp_datasources_enabled=lambda: True,
            validate_mcp_datasource=lambda _url, _creds: None,
            connector_drivers=SimpleNamespace(for_type=lambda _type: Driver()),
            connector_credentials=connector_credentials,
        ),
    )
    assert result["status"] == "ok"
    assert seen["authorized"] == [str(row["id"])]
    assert seen["credentials"] == {"username": "graph", "password": "secret-pass"}


# =============================================================================
# The authority for a Connector's own secret (decision 11)
# =============================================================================


CONNECTOR_ID = UUID("00000000-0000-4000-8000-0000000000d1")
ACCOUNT = {"kind": "Account", "name": OWNER}


def _connector(scope=ACCOUNT):
    return {
        "id": CONNECTOR_ID,
        "kind": "Connector",
        "linked_id": CONNECTOR_ID,
        "document": {"metadata": {"name": "db-000000000000", "scope": dict(scope)}},
    }


def _own_ref(scope=ACCOUNT):
    return {
        "name": connector_secret_name(CONNECTOR_ID),
        "key": "url",
        "scope": dict(scope),
    }


class TestTheConnectorSecretAuthority:
    def _authority(self, monkeypatch, *, allows: bool, user_id=OWNER):
        authority = ManifestAuthority(MagicMock(), {"id": user_id, "is_admin": False})
        policy = AsyncMock(return_value=allows)
        monkeypatch.setattr(
            "orchestrator.services.manifest_authority.connector_policy_authorizes",
            policy,
        )
        authority.secret = AsyncMock(
            side_effect=HTTPException(
                403, "Account resource belongs to a different user."
            )
        )

        async def deny(detail):
            raise HTTPException(403, detail)

        authority.deny = deny
        return authority, policy

    @pytest.mark.asyncio
    async def test_the_policy_lends_the_connectors_own_secret(self, monkeypatch):
        authority, policy = self._authority(monkeypatch, allows=True, user_id="u-2")
        scope = await authority.connector_secret(
            _own_ref(), _connector(), project_ids=["p-1"]
        )
        assert scope == ACCOUNT
        policy.assert_awaited_once_with(
            authority.db, authority.user, CONNECTOR_ID, ["p-1"]
        )
        authority.secret.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_refusal_of_the_policy_is_final(self, monkeypatch):
        """Write access to the secret's scope lends nothing the policy
        refused: not the owner's connector outside its projects, not a
        project's knowledge base to an editor's work outside the project."""
        authority, _policy = self._authority(monkeypatch, allows=False)
        authority.secret = AsyncMock(return_value=ACCOUNT)
        with pytest.raises(HTTPException) as refused:
            await authority.connector_secret(_own_ref(), _connector())
        assert (refused.value.status_code, refused.value.detail) == (
            403,
            GENERIC_UNAVAILABLE_DETAIL,
        )
        authority.secret.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_an_edit_of_its_own_document_falls_back_to_the_scope_rule(
        self, monkeypatch
    ):
        """A manifest apply of the Connector's own document reaches the
        store's refusal when the caller could write it."""
        authority, _policy = self._authority(monkeypatch, allows=False)
        authority.secret = AsyncMock(return_value=ACCOUNT)
        assert (
            await authority.connector_secret(_own_ref(), _connector(), edit=True)
            == ACCOUNT
        )
        authority.secret.assert_awaited_once_with(ACCOUNT)

    @pytest.mark.asyncio
    async def test_the_policy_is_asked_once_per_connector_and_projects(
        self, monkeypatch
    ):
        authority, policy = self._authority(monkeypatch, allows=True)
        for key in ("url", "shape", "password"):
            await authority.connector_secret(
                {**_own_ref(), "key": key}, _connector(), project_ids=["p-1"]
            )
        await authority.connector_secret(_own_ref(), _connector(), project_ids=[])
        assert policy.await_count == 2

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "ref",
        [
            {**_own_ref(), "name": "connector-" + "f" * 32},
            {**_own_ref(), "scope": {"kind": "Account", "name": "someone-else"}},
        ],
        ids=["another_name", "another_scope"],
    )
    async def test_another_secret_is_never_lent(self, monkeypatch, ref):
        authority, policy = self._authority(monkeypatch, allows=True)
        with pytest.raises(HTTPException):
            await authority.connector_secret(ref, _connector())
        policy.assert_not_awaited()
        authority.secret.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_a_catalog_secret_is_refused(self, monkeypatch):
        authority, policy = self._authority(monkeypatch, allows=True)
        catalog = {"kind": "Catalog", "name": "shared"}
        with pytest.raises(HTTPException) as refused:
            await authority.connector_secret(_own_ref(catalog), _connector(catalog))
        assert refused.value.detail == CATALOG_SECRET_DETAIL
        policy.assert_not_awaited()

    def test_only_a_linked_connectors_own_secret_counts(self):
        assert is_own_connector_secret(_own_ref(), _connector())
        assert not is_own_connector_secret(
            _own_ref(), {**_connector(), "linked_id": None}
        )
        assert not is_own_connector_secret(
            _own_ref(), {**_connector(), "kind": "Expert"}
        )


class TestNoOtherResourceUsesAConnectorSecret:
    def _authority(self):
        authority = ManifestAuthority(MagicMock(), {"id": OWNER, "is_admin": True})
        authority.scope = AsyncMock(return_value=ACCOUNT)

        async def deny(detail):
            raise HTTPException(403, detail)

        authority.deny = deny
        return authority

    @pytest.mark.asyncio
    async def test_a_reference_by_name_is_refused_even_to_its_owner(self):
        authority = self._authority()
        with pytest.raises(HTTPException) as refused:
            await authority.secret(ACCOUNT, name=connector_secret_name(CONNECTOR_ID))
        assert refused.value.detail == FOREIGN_CONNECTOR_SECRET_DETAIL
        authority.scope.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_any_other_name_keeps_the_scope_rule(self):
        authority = self._authority()
        assert await authority.secret(ACCOUNT, name="github") == ACCOUNT
        authority.scope.assert_awaited_once_with(ACCOUNT, write=True)

    @pytest.mark.asyncio
    async def test_another_connectors_secret_in_a_linked_connector_is_refused(self):
        authority = self._authority()
        ref = {**_own_ref(), "name": "connector-" + "e" * 32}
        with pytest.raises(HTTPException) as refused:
            await authority.connector_secret(ref, _connector())
        assert refused.value.detail == FOREIGN_CONNECTOR_SECRET_DETAIL

    @pytest.mark.asyncio
    async def test_only_an_edit_keeps_the_scope_rule_for_its_own(self, monkeypatch):
        authority = self._authority()
        monkeypatch.setattr(
            "orchestrator.services.manifest_authority.connector_policy_authorizes",
            AsyncMock(return_value=False),
        )
        with pytest.raises(HTTPException) as refused:
            await authority.connector_secret(_own_ref(), _connector())
        assert refused.value.detail == GENERIC_UNAVAILABLE_DETAIL
        assert (
            await authority.connector_secret(_own_ref(), _connector(), edit=True)
            == ACCOUNT
        )


@pytest.mark.asyncio
async def test_the_policy_check_is_one_classification_of_one_id(monkeypatch):
    from orchestrator.services import datasource_policy

    seen = {}

    async def classify(db, actor, owner, ids, projects, backend, **kwargs):
        seen.update(owner=owner, ids=ids, projects=projects, backend=backend, **kwargs)
        return [datasource_policy.ItemVerdict(ids[0], False)], {ids[0]: 1}

    monkeypatch.setattr(datasource_policy, "classify_datasource_selection", classify)
    assert await connector_secrets.connector_policy_authorizes(
        MagicMock(), {"id": OWNER}, CONNECTOR_ID, ["p-1"]
    )
    assert seen == {
        "owner": OWNER,
        "ids": [str(CONNECTOR_ID)],
        "projects": ["p-1"],
        "backend": None,
        # The override a session's or a job's own selection gets.
        "allow_admin_explicit_override": True,
    }

    async def unavailable(*_args, **_kwargs):
        raise datasource_policy.DatasourceUnavailableError()

    monkeypatch.setattr(datasource_policy, "classify_datasource_selection", unavailable)
    assert not await connector_secrets.connector_policy_authorizes(
        MagicMock(), {"id": OWNER}, CONNECTOR_ID
    )


@pytest.mark.asyncio
async def test_the_resource_api_refuses_a_connector_secret(monkeypatch):
    from orchestrator.services import manifest_resources
    from orchestrator.services.manifest_resources import ManifestResourceService

    service = ManifestResourceService.__new__(ManifestResourceService)
    service.db = MagicMock()
    authority = MagicMock()
    authority.scope = AsyncMock(return_value=ACCOUNT)
    authority.secret = AsyncMock(return_value=ACCOUNT)
    monkeypatch.setattr(
        manifest_resources, "ManifestAuthority", lambda *_a, **_k: authority
    )
    with pytest.raises(HTTPException) as refused:
        await service.put_secret(
            {"id": OWNER},
            scope=ACCOUNT,
            name=connector_secret_name(CONNECTOR_ID),
            values={"url": "x"},
        )
    assert refused.value.status_code == 409
    assert refused.value.detail == CONNECTOR_SECRET_DETAIL
    service.db.transaction_scope.assert_not_called()
