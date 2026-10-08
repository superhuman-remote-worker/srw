"""The agent's connector materializers (``agent.connectors``, slice D1b).

The wire stays today's payload; the agent reads each entry as a binding
descriptor (``binding_from_legacy_entry``) and routes it by delivery form.
These tests pin the mapping for every built-in type against the shared
binding schema, the routing, and each materializer's own behaviour. The
README lines are pinned by the facts golden, the runtime order by
``tests/test_agent_connectors_registry.py``.
"""

from __future__ import annotations

import logging
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from agent.connectors import (
    RuntimeContext,
    binding_from_legacy_entry,
    connector_registry,
    deliveries_from_payload,
)
from agent.connectors.connections import CONNECTION_FACTORIES, open_connection
from agent.connectors.env import EnvFileMaterializer
from agent.connectors.files import CredentialFileMaterializer
from agent.connectors.knowledge import KnowledgeIndexMaterializer
from agent.connectors.legacy import effective_access
from agent.connectors.mcp import McpClientMaterializer
from agent.connectors.ssh_identity import SshIdentityMaterializer
from orchestrator.services import agent_datasource_payload as payload_module
from orchestrator.services.connector_drivers import builtin_connector_drivers
from shared.connectors.binding import (
    BindingDescriptor,
    load_binding_schema,
    validate_binding,
)
from shared.connectors.builtin import (
    DATASOURCE_SPECS,
    driver_spec_for_row,
    spec_for_type,
)
from tests._connector_goldens import KINDS, SSH_PRIVATE_KEY, all_rows, resolved_row


def _payload(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """What an orchestrator sends for these rows (MCP enabled)."""
    return payload_module.build_datasources_payload(
        [dict(row) for row in rows],
        dependencies=payload_module.DatasourcePayloadDependencies(
            logger=SimpleNamespace(warning=lambda *_args, **_kwargs: None),
            mcp_datasources_enabled=lambda: True,
            mcp_stdio_enabled=lambda: True,
            connector_drivers=builtin_connector_drivers(),
            workspace_ssh_known_hosts=lambda: "",
        ),
    )


def _by_kind(**over: Any) -> dict[str, dict[str, Any]]:
    entries = _payload(all_rows(**over))
    assert len(entries) == len(KINDS)
    return dict(zip(KINDS, entries))


def _forms(descriptor: BindingDescriptor) -> list[str]:
    return [entry.form for entry in descriptor.entries]


def _value(descriptor: BindingDescriptor, form: str) -> dict[str, Any]:
    (value,) = [entry.value for entry in descriptor.entries if entry.form == form]
    return dict(value)


# =============================================================================
# binding_from_legacy_entry
# =============================================================================


@pytest.mark.parametrize("read_only", [False, True])
def test_every_canonical_entry_reads_as_a_valid_descriptor(read_only):
    jsonschema = pytest.importorskip("jsonschema")
    schema = load_binding_schema()
    for kind, entry in _by_kind(project_read_only=read_only).items():
        descriptor = binding_from_legacy_entry(entry)
        assert descriptor is not None, kind
        spec = spec_for_type(entry["type"])
        assert descriptor.driver == driver_spec_for_row(entry).name
        assert descriptor.name == entry["name"]
        assert set(_forms(descriptor)) <= set(spec.delivery_forms), kind
        document = descriptor.to_json()
        assert validate_binding(document) == [], kind
        jsonschema.validate(document, schema)


def test_a_remote_mcp_entry_names_the_remote_driver():
    entries = _by_kind()
    remote = binding_from_legacy_entry(entries["mcp_remote"])
    stdio = binding_from_legacy_entry(entries["mcp_stdio"])
    assert (remote.driver, stdio.driver) == ("srw.mcp-remote/v1", "srw.mcp/v1")
    # One driver implementation, so the same delivery either way.
    assert _forms(remote) == _forms(stdio) == ["mcp_client"]


def test_reading_an_entry_never_mutates_it():
    for entry in _by_kind().values():
        before = repr(entry)
        binding_from_legacy_entry(entry)
        assert repr(entry) == before


def test_the_descriptor_carries_each_forms_values():
    entries = _by_kind()

    generic = binding_from_legacy_entry(entries["generic"])
    assert _forms(generic) == ["env_file"] * len(
        entries["generic"]["credentials"]["env_vars"]
    )
    assert all(entry.recipient == "workspace" for entry in generic.entries)
    assert {entry.refresh for entry in generic.entries} == {"on_backend_swap"}

    kube = _value(binding_from_legacy_entry(entries["kubeconfig"]), "credential_file")
    assert kube["transform"] == "kubeconfig_prefix"
    assert kube["merge_group"] == "kubeconfig"
    assert kube["mode"] == 0o600
    (kube_entry,) = binding_from_legacy_entry(entries["kubeconfig"]).entries
    # The workspace, never the agent pod (D1d); it follows a backend swap.
    assert kube_entry.recipient == "workspace"
    assert kube_entry.refresh == "on_backend_swap"

    files = binding_from_legacy_entry(entries["generic_file"])
    assert [value["transform"] for value in (e.value for e in files.entries)] == [
        None,
        None,
    ]
    assert [e.value["mode"] for e in files.entries] == [0o600, 0o644]

    token = _value(binding_from_legacy_entry(entries["repository_token"]), "checkout")
    assert token["auth"] == "token_in_url"
    # The clone reads the token from the entry; no second copy.
    assert "secret" not in token
    assert "ghp_widgets-secret" not in repr(
        binding_from_legacy_entry(entries["repository_token"]).to_json()
    )
    assert token["forge"] == "github"
    assert token["datasource_id"] == entries["repository_token"]["datasource_id"]

    ssh = _value(binding_from_legacy_entry(entries["repository_ssh"]), "checkout")
    assert ssh["auth"] == "ssh_agent"
    assert "secret" not in ssh

    pg = _value(binding_from_legacy_entry(entries["postgresql"]), "managed_connection")
    assert pg["kind"] == "postgresql"
    assert pg["read_only"] is False

    stdio = _value(binding_from_legacy_entry(entries["mcp_stdio"]), "mcp_client")
    assert stdio == {
        "transport": "stdio",
        "url": None,
        "command": "npx",
        "args": ["-y", "@acme/mcp"],
    }

    kb = _value(binding_from_legacy_entry(entries["kb_native"]), "knowledge_index")
    assert kb["native_project_id"] is not None


def test_an_ssh_key_travels_apart_from_its_descriptor():
    """C1: the key rides workspace_ssh_identities, never the entry."""
    entries = _by_kind()
    for kind in ("ssh_key", "repository_ssh"):
        descriptor = binding_from_legacy_entry(entries[kind])
        assert "ssh_identity" not in _forms(descriptor)
        assert SSH_PRIVATE_KEY.strip() not in repr(descriptor.to_json())
    assert binding_from_legacy_entry(entries["ssh_key"]).entries == ()


@pytest.mark.parametrize(
    "entry",
    [
        {"type": "generic", "credentials": "not-a-dict"},
        {"type": "generic", "credentials": {"env_vars": ["A"]}},
        {"type": "kubeconfig", "credentials": {"files": ["x", None]}},
        {"type": "kubeconfig", "credentials": {"files": [{"mode": "999"}]}},
        {"type": "repository"},
        {"type": "email", "config": "x"},
        {"type": "mcp", "credentials": {"args": [1, 2]}},
        {"type": "kb", "config": None},
    ],
)
def test_a_malformed_entry_never_raises(entry):
    descriptor = binding_from_legacy_entry(entry)
    assert descriptor is not None
    assert descriptor.name == "unnamed"


def test_a_type_no_driver_serves_has_no_descriptor():
    assert binding_from_legacy_entry({"type": "redis", "name": "Cache"}) is None
    (delivery,) = deliveries_from_payload([{"type": "redis", "name": "Cache"}])
    assert delivery.binding is None and delivery.primary_form is None
    assert not delivery.routes_to("managed_connection")


@pytest.mark.parametrize(
    ("entry", "access"),
    [
        ({"type": "postgresql"}, "ReadWrite"),
        ({"type": "postgresql", "project_read_only": True}, "ReadOnly"),
        ({"type": "email"}, "draft"),
        ({"type": "email", "config": {"access": "send"}}, "send"),
        ({"type": "email", "config": {"access": "bogus"}}, "read"),
        (
            {"type": "email", "config": {"access": "send"}, "project_read_only": True},
            "read",
        ),
        ({"type": "kb"}, "ReadOnly"),
        ({"type": "mcp", "project_read_only": True}, "ReadWrite"),
        ({"type": "generic", "project_read_only": True}, "ReadOnly"),
    ],
)
def test_effective_access_follows_the_driver_levels(entry, access):
    from shared.datasource_policy import email_effective_access

    assert effective_access(entry, spec_for_type(entry["type"])) == access
    if entry["type"] == "email":
        assert email_effective_access(entry) == access


def test_deliveries_keep_the_wire_entries_and_payload_order():
    entries = [{"type": "kb", "name": "A"}, "junk", {"type": "mcp", "name": "B"}]
    deliveries = deliveries_from_payload(entries)
    assert [d.entry for d in deliveries] == [entries[0], entries[2]]
    assert deliveries[0].entry is entries[0]
    assert [d.index for d in deliveries] == [0, 1]


@pytest.mark.parametrize(
    ("legacy_type", "primary"),
    [
        ("generic", "env_file"),
        ("credentials", "env_file"),
        ("repository", "checkout"),
        ("kb", "knowledge_index"),
        ("postgresql", "managed_connection"),
        ("email", "managed_connection"),
        ("mcp", "mcp_client"),
        ("kubeconfig", "credential_file"),
        ("generic_file", "credential_file"),
        ("ssh_key", "ssh_identity"),
    ],
)
def test_each_type_is_described_by_its_first_form(legacy_type, primary):
    (delivery,) = deliveries_from_payload([{"type": legacy_type}])
    assert delivery.primary_form == primary


# =============================================================================
# Managed connections: a closed set of factories
# =============================================================================


def test_every_managed_driver_has_exactly_one_connection_factory():
    managed = {
        spec.legacy_type
        for spec in DATASOURCE_SPECS
        if "managed_connection" in spec.delivery_forms
    }
    assert set(CONNECTION_FACTORIES) == managed


def test_an_unknown_connection_kind_is_refused():
    with pytest.raises(ValueError, match="No connection factory"):
        open_connection({"kind": "redis"})


def test_the_email_factory_clamps_a_read_only_link_to_read():
    entry = {
        "type": "email",
        "credentials": {"username": "u"},
        "config": {"access": "send", "folders": ["INBOX"]},
        "project_read_only": True,
    }
    value = _value(binding_from_legacy_entry(entry), "managed_connection")
    connection, client = open_connection(value)
    assert client is None
    assert connection.access == "read"


# =============================================================================
# Environment files
# =============================================================================


def _workspace(installed: list) -> SimpleNamespace:
    return SimpleNamespace(
        backend=SimpleNamespace(
            supports_shell=True, install_credential_environment=installed.append
        )
    )


def test_a_backend_swap_installs_the_environment_on_the_new_backend():
    installed: list = []
    deliveries = deliveries_from_payload(
        [{"type": "credentials", "credentials": {"env_vars": {"K": "v"}}}]
    )
    EnvFileMaterializer().on_backend_swap(deliveries, _workspace(installed).backend)
    assert installed == [{"K": "v"}]
    EnvFileMaterializer().on_backend_swap([], _workspace(installed).backend)
    assert installed == [{"K": "v"}]


def test_a_live_change_installs_the_new_set():
    installed: list = []
    old = deliveries_from_payload(
        [{"type": "generic", "credentials": {"env_vars": {"OLD": "1"}}}]
    )
    new = deliveries_from_payload(
        [{"type": "generic", "credentials": {"env_vars": {"NEW": "2"}}}]
    )
    EnvFileMaterializer().replace(
        old,
        new,
        RuntimeContext(execution="session", workspace_manager=_workspace(installed)),
    )
    assert installed == [{"NEW": "2"}]


def test_a_detached_registered_driver_s_variables_are_unset():
    """Its driver revokes the credential behind them (D6): the workspace
    stops setting them for new commands. Other connectors' stay, as agreed
    for v1."""
    installed: list = []
    unset: list = []
    workspace = _workspace(installed)
    workspace.backend.unset_credential_environment = unset.append
    driver = {
        "type": "image_driver",
        "datasource_id": "c1",
        "credentials": {"env_vars": {"ACME_TOKEN": "minted", "ACME_URL": "u"}},
    }
    other = {"type": "generic", "credentials": {"env_vars": {"OLD": "1"}}}
    kept = {"type": "image_driver", "datasource_id": "c2"}
    kept["credentials"] = {"env_vars": {"KEPT": "k"}}
    EnvFileMaterializer().replace(
        deliveries_from_payload([driver, other, kept]),
        deliveries_from_payload([kept]),
        RuntimeContext(execution="session", workspace_manager=workspace),
    )
    assert unset == [["ACME_TOKEN", "ACME_URL"]]
    assert installed == [{"KEPT": "k"}]
    # An older backend without the method keeps them (and the revoke still
    # kills the credential).
    del workspace.backend.unset_credential_environment
    EnvFileMaterializer().replace(
        deliveries_from_payload([driver]),
        [],
        RuntimeContext(execution="session", workspace_manager=workspace),
    )


# =============================================================================
# Credential files
# =============================================================================


def test_credential_files_go_to_the_workspace_backend():
    """Never the agent pod: the files ride the workspace's own transport.

    The planning, the workspace program and the failure paths are pinned in
    ``tests/test_credential_file_materialization.py``.
    """
    synced: list = []
    backend = SimpleNamespace(
        supports_shell=True,
        install_credential_files=lambda files, env: synced.append(files) or {},
    )
    rt = RuntimeContext(
        execution="session", workspace_manager=SimpleNamespace(backend=backend)
    )
    CredentialFileMaterializer().materialize(
        deliveries_from_payload([resolved_row("generic_file")]), rt
    )
    (files,) = synced
    assert [item["link"] for item in files] == [
        ".config/gcloud/sa.json",
        ".config/gcloud/ca.pem",
    ]


# =============================================================================
# SSH identities (C1, unchanged behind the materializer)
# =============================================================================


@pytest.mark.parametrize(
    ("execution", "prunes"), [("worker", False), ("session", True)]
)
def test_identities_load_with_the_workspace_and_only_a_session_prunes(
    execution, prunes
):
    backend = SimpleNamespace(supports_shell=True)
    payloads = [{"authority_id": "a", "private_key": "secret"}]
    rt = RuntimeContext(execution=execution, backend=backend, ssh_identities=payloads)
    with (
        patch(
            "shared.runtime.core.workspace_ssh_identity."
            "materialize_workspace_ssh_identities",
            return_value={"a": "ready"},
        ) as materialize,
        patch(
            "shared.runtime.core.workspace_ssh_identity.prune_workspace_ssh_identities",
            return_value=True,
        ) as prune,
    ):
        SshIdentityMaterializer().materialize([], rt)

    materialize.assert_called_once_with(payloads, backend)
    assert rt.ssh_identities is None
    assert rt.ssh_identity_status == {"a": "ready"}
    assert prune.called is prunes
    if prunes:
        prune.assert_called_once_with(["a"], backend)


def test_a_live_detach_retires_only_the_detached_identity():
    def ssh(name, authority):
        return {
            "type": "ssh_key",
            "name": name,
            "ssh_identity": {
                "alias": f"srw-repo-{authority}",
                "authority_id": authority,
            },
        }

    kept, detached = ssh("Kept", "kept"), ssh("Gone", "gone")
    backend = SimpleNamespace(supports_shell=True)
    status = {"kept": "ready", "gone": "ready"}
    rt = RuntimeContext(
        execution="session", backend=backend, ssh_identity_status=status
    )
    with patch(
        "shared.runtime.core.workspace_ssh_identity.retire_workspace_ssh_identities",
        return_value=True,
    ) as retire:
        SshIdentityMaterializer().replace(
            deliveries_from_payload([kept, detached]),
            deliveries_from_payload([{**kept, "name": "Renamed"}]),
            rt,
        )
    retire.assert_called_once_with(["gone"], backend)
    assert status == {"kept": "ready"}
    assert rt.ssh_identity_status is status


# =============================================================================
# MCP and knowledge bases
# =============================================================================


@pytest.mark.asyncio
async def test_mcp_discovery_failure_never_fails_the_execution(caplog):
    manager = MagicMock()
    manager.connect_all = AsyncMock(side_effect=RuntimeError("boom"))
    rt = RuntimeContext(execution="session", connections={"mcp": manager})
    with caplog.at_level(logging.WARNING):
        await McpClientMaterializer().ready(rt)
    manager.annotate_configs.assert_called_once()
    assert "Unexpected MCP discovery failure" in caplog.text


def test_mcp_servers_share_one_manager_in_the_mcp_slot():
    rt = RuntimeContext(execution="worker")
    deliveries = deliveries_from_payload(
        [
            {"type": "mcp", "name": "A", "connection_url": "https://a/mcp"},
            {"type": "mcp", "name": "B", "connection_url": "https://b/mcp"},
        ]
    )
    McpClientMaterializer().materialize(deliveries, rt)
    assert list(rt.connections) == ["mcp"]
    assert len(rt.connections["mcp"]._handles) == 2


def test_a_live_kb_change_is_deferred_to_the_next_attach():
    materializer = KnowledgeIndexMaterializer()
    kb = {"type": "kb", "name": "Docs", "datasource_id": "kb-1"}
    rt = RuntimeContext(execution="session")
    materializer.replace(
        deliveries_from_payload([kb]), deliveries_from_payload([kb]), rt
    )
    assert rt.deferred == set()
    materializer.replace([], deliveries_from_payload([kb]), rt)
    assert rt.deferred == {"knowledge_index"}


def test_knowledge_bindings_see_only_kb_connectors():
    deliveries = deliveries_from_payload(
        [
            {"type": "postgresql", "name": "PG"},
            {
                "type": "kb",
                "name": "Docs",
                "datasource_id": "00000000-0000-0000-0000-0000000000d9",
            },
        ]
    )
    bindings = connector_registry().knowledge_bindings(
        deliveries, project_ids=[], runtime_actor=None
    )
    assert [binding.name for binding in bindings] == ["Docs"]


# =============================================================================
# README facts
# =============================================================================


def test_facts_list_an_unserved_type_under_other():
    lines = connector_registry().facts(
        deliveries_from_payload([{"type": "redis", "name": "Cache"}]),
        RuntimeContext(execution="session"),
    )
    assert lines == ["### Other", "- **Cache** (redis)", ""]


def test_facts_state_the_empty_set():
    lines = connector_registry().facts([], RuntimeContext(execution="session"))
    assert lines == ["_No connectors attached._", ""]


def test_knowledge_bindings_skip_a_connector_that_is_no_knowledge_base():
    """A caller passing a whole payload never binds another connector."""
    from agent.services.knowledge.bindings import build_knowledge_bindings

    bindings = build_knowledge_bindings(
        datasources=[
            {
                "type": "postgresql",
                "name": "PG",
                "datasource_id": "00000000-0000-0000-0000-0000000000d8",
            },
            {
                "type": "kb",
                "name": "Docs",
                "datasource_id": "00000000-0000-0000-0000-0000000000d9",
            },
        ]
    )
    assert [binding.name for binding in bindings] == ["Docs"]


@pytest.mark.parametrize(
    ("execution", "clones"), [("session", False), ("worker", True)]
)
def test_checkouts_without_a_workspace(execution, clones, caplog):
    """A session skips quietly, as it did; a worker reports it."""
    from agent.connectors.checkout import CheckoutMaterializer

    deliveries = deliveries_from_payload([resolved_row("repository_token")])
    with (
        patch("agent.connectors.checkout.clone_repository_datasources") as clone,
        caplog.at_level(logging.WARNING),
    ):
        CheckoutMaterializer().materialize(
            deliveries, RuntimeContext(execution=execution)
        )
    assert clone.called is clones
    assert caplog.text == ""


# =============================================================================
# Harness slots: the tools ask by tool category
# =============================================================================


#: Which slot each tool category reads: the literal map, never one built by
#: the function under test.
SLOT_OF_CATEGORY = {
    "graph": "neo4j",
    "sql": "postgresql",
    "mongodb": "mongodb",
    "webdav": "webdav",
    "email": "email",
    "mcp": "mcp",
}


def test_every_connection_driver_has_one_slot_for_its_tool_category():
    from agent.connectors.slots import SLOT_BY_CATEGORY, connection_slot

    assert SLOT_BY_CATEGORY == SLOT_OF_CATEGORY
    # The slot is where the materializers put the connection.
    for spec in DATASOURCE_SPECS:
        if "managed_connection" in spec.delivery_forms:
            assert connection_slot(spec) in CONNECTION_FACTORIES
    assert connection_slot(spec_for_type("repository")) is None


def test_a_tool_finds_its_connection_by_category():
    from agent.tools.context import ToolContext

    pg, mcp = object(), object()
    context = ToolContext(datasources={"postgresql": pg, "mcp": mcp})
    assert context.connection_for("sql") is pg
    assert context.connection_for("mcp") is mcp
    assert context.has_connection_for("sql")
    assert not context.has_connection_for("graph")
    assert context.connection_for("repo") is None
    assert not hasattr(context, "get_datasource")


_MCP_TOOL = SimpleNamespace(
    name="docs_mcp__search", metadata={}, description="search the docs"
)


def _category_tool_names() -> dict[str, set[str]]:
    from shared.connectors.builtin import tool_map

    names: dict[str, set[str]] = {category: set() for category in SLOT_OF_CATEGORY}
    for entry in tool_map().values():
        category = entry["category"]
        if category not in names:
            continue
        for key in ("read", "write"):
            names[category].update(entry.get(key) or ())
        for tier in (entry.get("tiers") or {}).values():
            names[category].update(tier)
    names["mcp"] = {_MCP_TOOL.name}
    return names


def _slot_sentinels() -> dict[str, Any]:
    """A distinct stand-in in every slot; only the MCP one lists a tool."""
    sentinels = {
        slot: MagicMock(name=f"{slot}-sentinel") for slot in SLOT_OF_CATEGORY.values()
    }
    sentinels["mcp"].get_langchain_tools.return_value = [_MCP_TOOL]
    return sentinels


def _captured(tool: Any, sentinels: dict[str, Any]) -> set[str]:
    """The slots whose stand-in a bound tool's closure holds (nested too)."""
    import inspect

    found: set[str] = set()

    def walk(function: Any, depth: int) -> None:
        if depth > 4 or not inspect.isfunction(function):
            return
        for value in inspect.getclosurevars(function).nonlocals.values():
            found.update(
                slot for slot, sentinel in sentinels.items() if value is sentinel
            )
            walk(value, depth + 1)

    walk(getattr(tool, "func", None) or getattr(tool, "coroutine", None), 0)
    return found


@pytest.fixture
def mcp_registered():
    from agent.tools.registry import register_mcp_tools

    sentinels = _slot_sentinels()
    register_mcp_tools(sentinels["mcp"])
    try:
        yield sentinels
    finally:
        register_mcp_tools(None)


@pytest.mark.parametrize("category", sorted(SLOT_OF_CATEGORY))
def test_each_bound_tool_reads_its_own_categorys_connection(category, mcp_registered):
    """Every slot holds a distinct stand-in: a tool bound to another's fails."""
    from agent.tools.context import ToolContext
    from agent.tools.registry import load_tools

    sentinels = mcp_registered
    names = _category_tool_names()[category]
    bound = load_tools(sorted(names), ToolContext(datasources=dict(sentinels)))
    assert {tool.name for tool in bound} == names
    slot = SLOT_OF_CATEGORY[category]
    for tool in bound:
        if category == "mcp":
            # The MCP manager's own tool: fetched from the MCP slot.
            assert tool is _MCP_TOOL
        else:
            assert _captured(tool, sentinels) == {slot}, tool.name


@pytest.mark.parametrize("category", sorted(SLOT_OF_CATEGORY))
def test_a_category_binds_by_its_own_slot_alone(category, mcp_registered):
    """Only its own slot opens a category's gate; every other leaves it shut."""
    from agent.tools.context import ToolContext
    from agent.tools.registry import load_tools

    sentinels = mcp_registered
    names = _category_tool_names()[category]
    slot = SLOT_OF_CATEGORY[category]
    alone = load_tools(sorted(names), ToolContext(datasources={slot: sentinels[slot]}))
    assert {tool.name for tool in alone} == names
    others = {key: value for key, value in sentinels.items() if key != slot}
    assert load_tools(sorted(names), ToolContext(datasources=others)) == []


def test_load_tools_binds_a_category_only_with_its_connection():
    from agent.tools.context import ToolContext
    from agent.tools.registry import load_tools

    bound = load_tools(
        ["sql_query", "sql_schema"],
        ToolContext(datasources={"postgresql": MagicMock()}),
    )
    assert sorted(tool.name for tool in bound) == ["sql_query", "sql_schema"]
    assert load_tools(["sql_query"], ToolContext(datasources={})) == []


@pytest.mark.parametrize(
    ("datasources", "attached"),
    [
        ([{"type": "mcp", "name": "M"}], True),
        ([{"type": "postgresql"}, {"type": "kb"}], False),
        ([{"type": "MCP"}, "junk"], False),
        (None, False),
    ],
)
def test_a_worker_counts_mcp_by_delivery_form(datasources, attached):
    from agent.api.turn_executor import StatelessTurnExecutor

    request = SimpleNamespace(datasources=datasources)
    assert StatelessTurnExecutor._worker_mcp_attached(request) is attached
