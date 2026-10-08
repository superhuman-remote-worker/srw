"""The generated capability matrix (connector drivers D2).

``GET /api/datasources/drivers`` is assembled from the installed driver
registry alone.  Pinned here:

* every installed driver is listed, with its access levels and their
  ``enforced_by`` lines, trust, credential slots and egress columns;
* built-in drivers are "built-in, trusted"; anything else is marked as its
  author's word;
* no credential value can appear: the route reads no connector, and a
  ``writeOnly`` schema loses any value its author put there;
* any approved user may read it, and the literal segment wins over
  ``/{datasource_id}``;
* the response is the cockpit's fixture byte for byte, so the cockpit specs
  render what the API returns.  Regenerate the fixture with
  ``UPDATE_CONNECTOR_GOLDENS=1 python -m pytest tests/test_connector_capability_matrix.py``.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from orchestrator.services.connector_drivers import (
    ConnectorDriverRegistry,
    builtin_connector_drivers,
)
from orchestrator.services.connector_drivers.matrix import (
    capability_matrix,
    public_schema,
)
from shared.connectors.builtin import BUILTIN_SPECS, DATASOURCE_SPECS
from shared.connectors.contract import (
    AccessLevel,
    CredentialSlot,
    DriverSpec,
    EgressRule,
)
from tests._mounted_router import mount_router

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "cockpit/src/app/core/models/fixtures/connector-drivers.json"
UPDATE = os.environ.get("UPDATE_CONNECTOR_GOLDENS") == "1"

USER = {"id": "00000000-0000-0000-0000-0000000000c1", "is_admin": False}
STORED_SECRET = "hunter2-stored"
AUTHOR_SECRET = "hunter2-author-default"


class _ImageDriver:
    """A driver that is not SRW's own code (what D6 will register)."""

    def __init__(self, spec: DriverSpec) -> None:
        self.spec = spec


def _image_spec() -> DriverSpec:
    return DriverSpec(
        name="community.ticketing/v1",
        title="Ticketing",
        plane="bind_time",
        delivery_forms=("env_file",),
        config_schema={
            "type": "object",
            "properties": {
                "host": {"type": "string", "default": "tickets.example.com"},
                "token": {"type": "string", "writeOnly": True, "default": "x"},
            },
        },
        credential_slots=(
            CredentialSlot(
                "api",
                "secret_string",
                {
                    "type": "object",
                    "properties": {
                        "key": {
                            "type": "string",
                            "writeOnly": True,
                            "default": AUTHOR_SECRET,
                            "examples": [AUTHOR_SECRET],
                        }
                    },
                },
                required=True,
            ),
        ),
        access_levels=(
            AccessLevel("ReadOnly", 0, "The token's scope.", tools=("t_read",)),
        ),
        supported_backends=frozenset({"sandbox"}),
        workspace_requirements="A shell.",
        egress=(EgressRule("${config.host}", (443,)),),
        needs_dns="The host is a name.",
    )


def _store() -> SimpleNamespace:
    """Every read a connector route could make, each holding a secret."""
    row = {"id": "d1", "type": "postgresql", "credentials": {"p": STORED_SECRET}}
    return SimpleNamespace(
        list_datasources=AsyncMock(return_value=[row]),
        list_datasource_catalog=AsyncMock(return_value={"items": [row]}),
        list_eligible_datasources=AsyncMock(return_value=[row]),
        get_datasource=AsyncMock(return_value=row),
    )


def _client(*, registry=None, approved=None):
    from orchestrator.routers.datasources import DatasourcesDependencies, router
    from orchestrator.services.datasources import DatasourceDependencies

    store = _store()

    async def approved_user(_request, _store):
        return USER

    async def unreachable(*_args, **_kwargs):
        raise AssertionError("the matrix needs no connector gate")

    deps = DatasourcesDependencies(
        store=store,
        operations=DatasourceDependencies(
            store=store,
            vector_db=MagicMock(),
            knowledge_index=MagicMock(),
            mcp_datasources_enabled=lambda: False,
            mcp_stdio_enabled=lambda: False,
            validate_mcp_datasource=lambda _url, _creds: None,
            connector_drivers=registry or builtin_connector_drivers(),
        ),
        require_approved_user=approved or approved_user,
        require_project_member=unreachable,
        require_project_owner=unreachable,
        require_datasource_access=unreachable,
        require_datasource_owner=unreachable,
        require_job_access=unreachable,
    )
    app = mount_router(
        router, factories={"datasources_dependencies_factory": lambda: deps}
    )
    return SimpleNamespace(client=TestClient(app), store=store)


@pytest.fixture(scope="module")
def matrix():
    return capability_matrix(builtin_connector_drivers())


class TestBuiltinMatrix:
    def test_every_installed_driver_is_listed_in_registry_order(self, matrix):
        registry = builtin_connector_drivers()
        assert [d["name"] for d in matrix["drivers"]] == [
            spec.name for spec in registry.specs()
        ]
        assert {d["name"] for d in matrix["drivers"]} == {
            spec.name for spec in BUILTIN_SPECS
        }
        assert matrix["protocol_version"] == "1.0"

    @pytest.mark.parametrize("spec", DATASOURCE_SPECS, ids=lambda s: s.name)
    def test_access_levels_carry_their_enforced_by_lines(self, matrix, spec):
        entry = next(d for d in matrix["drivers"] if d["name"] == spec.name)
        assert entry["access_levels"], spec.name
        assert [level["id"] for level in entry["access_levels"]] == list(
            spec.ranked_access_ids()
        )
        for level in entry["access_levels"]:
            declared = spec.access_level(level["id"])
            assert level["enforced_by"] == declared.enforced_by
            assert level["advisory"] is declared.advisory
            assert level["rank"] == declared.rank
            assert level["tools"] == (
                "*" if declared.tools == "*" else list(declared.tools)
            )

    def test_spec_fields_are_carried(self, matrix):
        kb = next(d for d in matrix["drivers"] if d["name"] == "srw.kb/v1")
        assert kb["legacy_type"] == "kb"
        assert kb["plane"] == "harness"
        assert kb["delivery_forms"] == ["knowledge_index"]
        assert kb["supported_backends"] == ["none", "sandbox", "virtual", "vm"]
        assert kb["forced_read_only"] is True
        assert kb["default_access"] == "ReadOnly"
        assert kb["operations"] == ["status", "reindex"]
        assert kb["holds_upstream_credentials"] is True
        mcp = next(d for d in matrix["drivers"] if d["name"] == "srw.mcp/v1")
        assert [level["id"] for level in mcp["access_levels"]] == ["ReadWrite"]
        assert mcp["access_levels"][0]["tools"] == "*"
        assert mcp["deployment_gate"] == "mcp_datasources"
        env = next(d for d in matrix["drivers"] if d["name"] == "srw.env/v1")
        assert env["access_levels"] == []
        assert env["legacy_type"] is None

    def test_builtins_are_built_in_and_trusted(self, matrix):
        for entry in matrix["drivers"]:
            assert entry["trust"] == {
                "tier": "builtin",
                "trusted": True,
                "image": None,
                "claims_declared_by_author": False,
            }

    def test_builtins_run_in_process_so_egress_is_not_enforced_by_srw(self, matrix):
        for entry in matrix["drivers"]:
            assert entry["egress"] == {
                "declared": {"rules": [], "needs_dns": None},
                "enforced": {
                    "status": "not_applicable",
                    "reason": "runs_in_srw_process",
                },
                "installation": {
                    "status": "not_applicable",
                    "reason": "runs_in_srw_process",
                },
            }

    def test_credential_slots_describe_names_kinds_and_rules(self, matrix):
        email = next(d for d in matrix["drivers"] if d["name"] == "srw.email/v1")
        assert [
            (s["name"], s["kind"], s["required"], s["access_levels"])
            for s in email["credential_slots"]
        ] == [
            ("mailbox", "secret_string", True, []),
            ("smtp", "secret_string", False, ["send"]),
        ]
        password = email["credential_slots"][0]["schema"]["properties"]["password"]
        assert password == {"type": "string", "writeOnly": True}

    @pytest.mark.parametrize("spec", DATASOURCE_SPECS, ids=lambda s: s.name)
    def test_each_slot_names_its_key_names_field(self, matrix, spec):
        entry = next(d for d in matrix["drivers"] if d["name"] == spec.name)
        assert [s["names_field"] for s in entry["credential_slots"]] == [
            slot.names_field for slot in spec.credential_slots
        ]

    def test_the_credentials_slot_lists_its_names_under_env_var_names(self, matrix):
        credentials = next(
            d for d in matrix["drivers"] if d["name"] == "srw.credentials/v1"
        )
        assert credentials["credential_slots"][0]["names_field"] == "env_var_names"

    def test_the_matrix_is_json(self, matrix):
        assert json.loads(json.dumps(matrix)) == matrix


class TestDriversOutsideTheTrustedList:
    def test_every_claim_is_marked_as_the_authors(self):
        registry = ConnectorDriverRegistry([_ImageDriver(_image_spec())])
        (entry,) = capability_matrix(registry)["drivers"]
        assert entry["trust"] == {
            "tier": "custom",
            "trusted": False,
            "image": None,
            "claims_declared_by_author": True,
        }
        assert entry["egress"]["declared"] == {
            "rules": [{"host": "${config.host}", "ports": [443], "protocol": "tcp"}],
            "needs_dns": "The host is a name.",
        }
        assert entry["egress"]["enforced"]["status"] == "not_enforced"
        assert entry["egress"]["installation"]["status"] == "not_enforced"

    def test_an_in_process_driver_with_a_foreign_name_is_not_built_in(self):
        from orchestrator.services.connector_drivers.base import DatasourceDriver

        spec = DriverSpec(
            **{
                **{
                    f: getattr(DATASOURCE_SPECS[0], f)
                    for f in DATASOURCE_SPECS[0].__dataclass_fields__
                },
                "name": "acme.generic/v1",
            }
        )
        registry = ConnectorDriverRegistry([DatasourceDriver(spec)])
        (entry,) = capability_matrix(registry)["drivers"]
        assert entry["trust"]["claims_declared_by_author"] is True


class TestNoCredentialValue:
    def test_write_only_schemas_lose_their_values(self):
        schema = {
            "type": "object",
            "properties": {
                "host": {"type": "string", "default": "db.example.com"},
                "secret": {
                    "type": "object",
                    "writeOnly": True,
                    "default": {"k": "v"},
                    "properties": {"nested": {"type": "string", "examples": ["e"]}},
                },
            },
        }
        cleaned = public_schema(schema)
        assert cleaned["properties"]["host"]["default"] == "db.example.com"
        assert "default" not in cleaned["properties"]["secret"]
        assert "examples" not in cleaned["properties"]["secret"]["properties"]["nested"]

    def test_a_parent_default_holding_a_secret_childs_value_is_dropped(self):
        schema = {
            "type": "object",
            "default": {"user": "svc", "password": AUTHOR_SECRET},
            "examples": [{"password": AUTHOR_SECRET}],
            "properties": {
                "user": {"type": "string", "default": "svc"},
                "password": {"type": "string", "writeOnly": True},
            },
        }
        cleaned = public_schema(schema)
        assert "default" not in cleaned and "examples" not in cleaned
        assert AUTHOR_SECRET not in json.dumps(cleaned)
        # A sibling that holds no secret keeps its documentation.
        assert cleaned["properties"]["user"]["default"] == "svc"

    def test_const_and_enum_inside_a_secret_are_dropped(self):
        schema = {
            "type": "object",
            "properties": {
                "pin": {"type": "string", "writeOnly": True, "const": AUTHOR_SECRET},
                "auth": {
                    "type": "object",
                    "writeOnly": True,
                    "properties": {"key": {"enum": [AUTHOR_SECRET, "other"]}},
                },
                "mode": {"enum": ["a", "b"]},
                "backend": {"const": "imap_smtp"},
            },
        }
        cleaned = public_schema(schema)
        assert AUTHOR_SECRET not in json.dumps(cleaned)
        assert cleaned["properties"]["pin"] == {"type": "string", "writeOnly": True}
        assert cleaned["properties"]["auth"]["properties"]["key"] == {}
        assert cleaned["properties"]["mode"] == {"enum": ["a", "b"]}
        assert cleaned["properties"]["backend"] == {"const": "imap_smtp"}

    def test_a_secret_pointing_at_a_shared_definition_loses_its_default(self):
        schema = {
            "type": "object",
            "$defs": {"token": {"type": "string", "default": AUTHOR_SECRET}},
            "properties": {
                "token": {"$ref": "#/$defs/token", "writeOnly": True},
                "label": {"$ref": "#/$defs/token"},
            },
        }
        cleaned = public_schema(schema)
        assert "$defs" not in cleaned
        assert cleaned["properties"]["token"] == {"type": "string", "writeOnly": True}
        # The non-secret use of the same definition is inlined with its default.
        assert cleaned["properties"]["label"] == {
            "type": "string",
            "default": AUTHOR_SECRET,
        }
        assert "$ref" not in json.dumps(cleaned)

    @pytest.mark.parametrize(
        "ref",
        ["https://example.com/schema.json", "#/$defs/missing", "#/$defs/loop"],
    )
    def test_a_remote_missing_or_circular_ref_shows_nothing(self, ref):
        schema = {
            "type": "object",
            "$defs": {"loop": {"$ref": "#/$defs/loop", "default": AUTHOR_SECRET}},
            "properties": {"key": {"$ref": ref, "writeOnly": True}},
        }
        cleaned = public_schema(schema)
        assert cleaned["properties"]["key"] == {}
        assert AUTHOR_SECRET not in json.dumps(cleaned)

    def test_properties_named_like_keywords_survive_inside_a_secret(self):
        schema = {
            "type": "object",
            "writeOnly": True,
            "properties": {
                "default": {"type": "string"},
                "examples": {"type": "string"},
                "enum": {"type": "string"},
            },
        }
        cleaned = public_schema(schema)
        assert set(cleaned["properties"]) == {"default", "examples", "enum"}

    def test_an_authors_secret_default_never_reaches_the_response(self):
        wire = _client(registry=ConnectorDriverRegistry([_ImageDriver(_image_spec())]))
        response = wire.client.get("/api/datasources/drivers")
        assert response.status_code == 200
        assert AUTHOR_SECRET not in response.text
        assert '"x"' not in response.text
        # A non-secret default is the author's documentation and stays.
        assert "tickets.example.com" in response.text

    def test_the_route_reads_no_connector(self):
        wire = _client()
        response = wire.client.get("/api/datasources/drivers")
        assert response.status_code == 200
        assert STORED_SECRET not in response.text
        for name in vars(wire.store):
            getattr(wire.store, name).assert_not_awaited()


class TestRoute:
    def test_any_approved_user_reads_the_matrix(self, matrix):
        response = _client().client.get("/api/datasources/drivers")
        assert response.status_code == 200
        assert response.json() == matrix

    def test_an_unapproved_caller_is_refused(self):
        async def refuse(_request, _store):
            raise HTTPException(status_code=403, detail="not approved")

        response = _client(approved=refuse).client.get("/api/datasources/drivers")
        assert response.status_code == 403

    def test_the_literal_segment_wins_over_a_datasource_id(self):
        wire = _client()
        assert wire.client.get("/api/datasources/drivers").status_code == 200
        wire.store.get_datasource.assert_not_awaited()

    def test_the_cockpit_fixture_is_the_response(self):
        body = _client().client.get("/api/datasources/drivers").json()
        rendered = json.dumps(body, indent=2) + "\n"
        if UPDATE:
            FIXTURE.write_text(rendered)
        assert FIXTURE.read_text() == rendered, (
            "the cockpit fixture is stale; regenerate it with "
            "UPDATE_CONNECTOR_GOLDENS=1 python -m pytest "
            "tests/test_connector_capability_matrix.py"
        )
