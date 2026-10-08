"""The connector driver contract in ``shared.connectors``.

Specs, the binding descriptor with its JSON Schema, and the request/output
envelope image drivers will speak. The built-in specs are also checked
against the hand-maintained lists they replaced, so the catalogue, the tool
map and the delivery-form type sets keep saying the same thing.
"""

from __future__ import annotations

import ast
import copy
import json
import sys
from dataclasses import replace
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

from shared.connectors import (
    API_CHECK_STATUSES,
    BUILTIN_SPECS,
    DATASOURCE_SPECS,
    ERROR_CLASSES,
    LEGACY_TYPE_IDS,
    PROTOCOL_VERSION,
    AccessLevel,
    BindingDescriptor,
    BindingEntry,
    CredentialSlot,
    DriverError,
    DriverOutcome,
    DriverRequest,
    EnvelopeError,
    ExecutionRef,
    api_check_result,
    binding_schema,
    load_binding_schema,
    parse_output_line,
    read_output,
    spec_for_type,
    validate_binding,
    validate_request,
    validate_spec,
)
from shared.connectors.binding import VALUE_FIELDS
from shared.connectors.builtin import (
    FORGES,
    GENERIC_SPEC,
    delivers_in,
    legacy_types_where,
    legacy_types_with_config_key,
    legacy_types_with_form,
    needs_knowledge_profile,
    spec_for_row,
    tool_categories,
    tool_map,
)
from shared.connectors.contract import WORKSPACE_BACKENDS

_PACKAGE = Path(__file__).resolve().parents[1] / "src" / "shared" / "connectors"


# =============================================================================
# Specs
# =============================================================================


class TestBuiltinSpecs:
    @pytest.mark.parametrize("spec", BUILTIN_SPECS, ids=lambda spec: spec.name)
    def test_every_builtin_spec_is_valid(self, spec):
        assert validate_spec(spec) == []

    def test_names_and_types_are_unique(self):
        names = [spec.name for spec in BUILTIN_SPECS]
        assert len(names) == len(set(names))
        assert len(LEGACY_TYPE_IDS) == len(set(LEGACY_TYPE_IDS)) == 13
        assert all(spec.name.startswith("srw.") for spec in BUILTIN_SPECS)

    def test_every_datasource_spec_maps_back_to_its_type(self):
        for spec in DATASOURCE_SPECS:
            assert spec_for_type(spec.legacy_type) is spec
        assert spec_for_type("ftp") is None
        assert spec_for_type(None) is None

    def test_the_manifest_drivers_have_no_datasource_type_and_no_access(self):
        env, files = BUILTIN_SPECS[:2]
        assert (env.name, files.name) == ("srw.env/v1", "srw.files/v1")
        assert env.legacy_type is files.legacy_type is None
        assert env.access_levels == files.access_levels == ()

    def test_declared_read_only_is_advisory_on_env_and_file_connectors(self):
        for form in ("env_file", "credential_file", "ssh_identity"):
            for type_id in legacy_types_with_form(form):
                read_only = spec_for_type(type_id).access_level("ReadOnly")
                assert read_only.advisory, type_id

    def test_enforced_read_only_is_not_advisory_on_managed_connections(self):
        for type_id in ("postgresql", "neo4j", "mongodb", "webdav"):
            assert not spec_for_type(type_id).access_level("ReadOnly").advisory

    def test_vocabularies_match_their_owners(self):
        from shared.runtime.services.forge import SUPPORTED_FORGES
        from shared.workspace_contract import CANONICAL_WORKSPACE_BACKENDS

        assert WORKSPACE_BACKENDS == CANONICAL_WORKSPACE_BACKENDS
        assert set(FORGES) == set(SUPPORTED_FORGES)

    def test_email_tiers_rank_in_escalation_order(self):
        assert spec_for_type("email").ranked_access_ids() == (
            "read",
            "read_write",
            "draft",
            "send",
        )

    def test_config_schemas_and_slot_schemas_are_valid_json_schema(self):
        for spec in BUILTIN_SPECS:
            Draft202012Validator.check_schema(dict(spec.config_schema))
            for slot in spec.credential_slots:
                Draft202012Validator.check_schema(dict(slot.schema))


class TestValidateSpec:
    def test_reports_each_broken_field(self):
        broken = replace(
            GENERIC_SPEC,
            name="Generic",
            protocol_version="2.0",
            plane="cloud",
            delivery_forms=("telepathy",),
            supported_backends=frozenset({"sandbox", "mainframe"}),
            access_levels=(
                AccessLevel("ReadOnly", 0, ""),
                AccessLevel("ReadOnly", 0, "twice"),
            ),
            default_access="Admin",
            credential_slots=(
                CredentialSlot("x", "pigeon", {}, access_levels=("Admin",)),
            ),
            operations=("bind",),
        )
        problems = "\n".join(validate_spec(broken))
        for fragment in (
            "driver name 'Generic'",
            "protocol_version '2.0'",
            "plane 'cloud'",
            "delivery form 'telepathy'",
            "unknown workspace backends ['mainframe']",
            "access level ids are not unique",
            "access level ranks are not unique",
            "has no enforced_by line",
            "default_access 'Admin'",
            "kind 'pigeon'",
            "unknown access levels ['Admin']",
            "operation 'bind' is not an optional operation",
        ):
            assert fragment in problems

    @pytest.mark.parametrize(
        "name",
        ["srw.git/v1", "community.neo4j-mcp/v2", "acme.tools.crm/v10"],
    )
    def test_accepts_namespaced_names(self, name):
        assert validate_spec(replace(GENERIC_SPEC, name=name)) == []

    @pytest.mark.parametrize(
        "name", ["git/v1", "srw.git", "srw.git/v0", "SRW.git/v1", "srw.git/1"]
    )
    def test_refuses_malformed_names(self, name):
        assert validate_spec(replace(GENERIC_SPEC, name=name))

    def test_a_slot_names_field_must_be_a_field_name(self):
        slot = GENERIC_SPEC.credential_slots[0]
        for good in ("env_var_names", None):
            spec = replace(
                GENERIC_SPEC, credential_slots=(replace(slot, names_field=good),)
            )
            assert validate_spec(spec) == []
        spec = replace(
            GENERIC_SPEC, credential_slots=(replace(slot, names_field="Env Names"),)
        )
        assert "names_field is not a field name" in "\n".join(validate_spec(spec))

    @pytest.mark.parametrize("field", ["name", "created_by", "id", "type", "config"])
    def test_a_names_field_cannot_shadow_a_read_field(self, field):
        slot = replace(GENERIC_SPEC.credential_slots[0], names_field=field)
        problems = "\n".join(
            validate_spec(replace(GENERIC_SPEC, credential_slots=(slot,)))
        )
        assert f"names_field {field!r} is a connector read field" in problems

    @pytest.mark.parametrize(
        "schema",
        [
            {"type": "object", "properties": {"env_vars": {"type": "string"}}},
            {"type": "object", "properties": {"env_vars": {"type": "array"}}},
            {"type": "object", "properties": {}},
            {"type": "object"},
            {"type": "string"},
        ],
    )
    def test_a_names_field_needs_an_object_valued_slot(self, schema):
        slot = replace(
            GENERIC_SPEC.credential_slots[0], schema=schema, names_field="env_names"
        )
        problems = "\n".join(
            validate_spec(replace(GENERIC_SPEC, credential_slots=(slot,)))
        )
        assert "names_field needs an object-typed 'env_vars'" in problems

    def test_names_fields_are_unique_across_slots(self):
        first = replace(GENERIC_SPEC.credential_slots[0], names_field="env_names")
        second = replace(
            first,
            name="other_vars",
            schema={
                "type": "object",
                "properties": {"other_vars": {"type": "object"}},
            },
        )
        problems = validate_spec(
            replace(GENERIC_SPEC, credential_slots=(first, second))
        )
        assert "credential slot names_field values are not unique" in problems
        unique = replace(second, names_field="other_names")
        assert (
            validate_spec(replace(GENERIC_SPEC, credential_slots=(first, unique))) == []
        )


# =============================================================================
# What other code asks the specs (slice D1c)
# =============================================================================


class _Record:
    """A database record: ``get`` without being a ``Mapping``."""

    def __init__(self, **values):
        self._values = values

    def get(self, key, default=None):
        return self._values.get(key, default)


class TestSpecQueries:
    @pytest.mark.parametrize(
        ("row", "name"),
        [
            ({"type": "kb"}, "srw.kb/v1"),
            ({"type": "Repository"}, "srw.repository/v1"),
            (_Record(type="ssh_key"), "srw.ssh-key/v1"),
            ({"type": "ftp"}, None),
            ({}, None),
            (None, None),
            ("kb", None),
        ],
    )
    def test_spec_for_row(self, row, name):
        spec = spec_for_row(row)
        assert (spec.name if spec else None) == name

    def test_only_the_repository_checks_out(self):
        assert {
            spec.legacy_type
            for spec in DATASOURCE_SPECS
            if delivers_in({"type": spec.legacy_type}, "checkout")
        } == {"repository"}
        assert not delivers_in(None, "checkout")

    def test_only_the_kb_needs_the_knowledge_profile(self):
        assert {
            spec.legacy_type
            for spec in DATASOURCE_SPECS
            if needs_knowledge_profile({"type": spec.legacy_type})
        } == {"kb"}
        assert needs_knowledge_profile({"type": "KB"})
        assert not needs_knowledge_profile({"type": "ftp"})

    def test_the_behaviour_flags_name_the_types_they_replaced(self):
        assert legacy_types_where(lambda s: s.live_detach == "refused") == {
            "credentials"
        }
        assert legacy_types_where(lambda s: not s.delete_while_attached) == {
            "credentials"
        }
        assert legacy_types_where(lambda s: s.forced_read_only) == {"kb"}
        assert legacy_types_with_config_key("unattended_send") == {"email"}
        assert legacy_types_with_config_key("nothing-declares-this") == frozenset()

    def test_only_the_credentials_environment_shows_its_names(self):
        assert {
            (spec.legacy_type, slot.name, slot.names_field)
            for spec in DATASOURCE_SPECS
            for slot in spec.credential_slots
            if slot.names_field
        } == {("credentials", "env_vars", "env_var_names")}

    def test_tool_categories_follow_the_tool_map(self):
        assert tool_categories() == tuple(
            entry["category"] for entry in tool_map().values()
        )
        assert set(tool_categories()) == {
            "sql",
            "mongodb",
            "graph",
            "webdav",
            "email",
            "mcp",
            "repo",
        }


# =============================================================================
# Binding descriptor
# =============================================================================

_VALUES = {
    "env_file": {"name": "API_TOKEN", "value": "s3cret"},
    "credential_file": {
        "path": "/home/srw/.kube/configs/prod.yaml",
        "content": "apiVersion: v1",
        "mode": 384,
        "env_var": "KUBECONFIG",
        "transform": "kubeconfig_prefix",
        "merge_group": "kubeconfig",
    },
    "checkout": {
        "url": "https://github.com/acme/widgets.git",
        "name_hint": "widgets",
        "auth": "token_in_url",
        "secret": "ghp_x",
        "default_branch": "main",
        "require_default_branch": True,
        "forge": "github",
        "datasource_id": "d1",
        "read_only": False,
    },
    "managed_connection": {
        "kind": "neo4j",
        "url": "bolt://graph:7687",
        "credentials": {"username": "neo4j", "password": "pw"},
        "config": {},
        "read_only": True,
    },
    "mcp_client": {
        "transport": "stdio",
        "command": "npx",
        "args": ["-y", "server"],
        "env": {"K": "v"},
    },
    "knowledge_index": {"datasource_id": "d1", "config": {"root_path": "docs"}},
    "pod_env": {"name": "TOKEN", "value": "x"},
    "pod_file": {"path": "/run/srw/bindings/key", "content": "x"},
    "ssh_identity": {
        "alias": "srw-repo-" + "a" * 32,
        "authority_id": "00000000-0000-4000-8000-0000000000a2",
        "private_key": "key",
        "fingerprint": "SHA256:x",
        "host": "bastion.example.com",
        "port": 22,
        "user": "deploy",
        "known_hosts": ["ssh-ed25519 AAAA"],
    },
}


def _descriptor(form: str, **value_over) -> dict:
    return BindingDescriptor(
        driver="srw.test/v1",
        name="Test",
        access="ReadOnly",
        entries=(
            BindingEntry(
                recipient="workspace",
                form=form,
                value={**_VALUES[form], **value_over},
                collision="error",
            ),
        ),
    ).to_json()


@pytest.fixture(scope="module")
def schema_validator():
    schema = load_binding_schema()
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema)


class TestBindingDescriptor:
    def test_the_fixtures_cover_every_form(self):
        assert set(_VALUES) == set(VALUE_FIELDS)

    @pytest.mark.parametrize("form", sorted(VALUE_FIELDS))
    def test_a_valid_entry_passes_both_checks(self, form, schema_validator):
        descriptor = _descriptor(form)
        assert validate_binding(descriptor) == []
        assert list(schema_validator.iter_errors(descriptor)) == []

    @pytest.mark.parametrize(
        ("form", "over"),
        [
            ("env_file", {"value": 7}),
            ("credential_file", {"mode": True}),
            ("credential_file", {"transform": "rot13"}),
            ("checkout", {"auth": "password"}),
            ("checkout", {"read_only": "yes"}),
            ("managed_connection", {"credentials": "pw"}),
            ("mcp_client", {"transport": "websocket"}),
            ("knowledge_index", {"surprise": 1}),
            ("pod_file", {"path": None}),
        ],
    )
    def test_a_broken_value_fails_both_checks(self, form, over, schema_validator):
        descriptor = _descriptor(form, **over)
        assert validate_binding(descriptor)
        assert list(schema_validator.iter_errors(descriptor))

    @pytest.mark.parametrize("missing", ["name", "url"])
    def test_a_missing_required_field_fails_both_checks(
        self, missing, schema_validator
    ):
        form = "env_file" if missing == "name" else "checkout"
        descriptor = _descriptor(form)
        del descriptor["entries"][0]["value"][missing]
        assert validate_binding(descriptor)
        assert list(schema_validator.iter_errors(descriptor))

    @pytest.mark.parametrize(
        "broken",
        [
            {"recipient": "laptop"},
            {"collision": "merge"},
            {"retire": "burn"},
            {"form": "carrier_pigeon"},
            {"extra": 1},
        ],
    )
    def test_entry_vocabulary_is_closed_in_both_checks(self, broken, schema_validator):
        descriptor = _descriptor("pod_env")
        descriptor["entries"][0].update(broken)
        assert validate_binding(descriptor)
        assert list(schema_validator.iter_errors(descriptor))

    def test_descriptor_fields_are_checked(self, schema_validator):
        descriptor = _descriptor("pod_env")
        descriptor["driver"] = "not a driver"
        descriptor["entries"] = {}
        assert validate_binding(descriptor)
        assert list(schema_validator.iter_errors(descriptor))

    def test_secret_values_are_kept_out_of_repr(self):
        entry = BindingEntry("workspace", "env_file", {"name": "K", "value": "s3cret"})
        assert "s3cret" not in repr(entry)

    def test_the_schema_ships_as_package_data(self):
        pyproject = (Path(__file__).resolve().parents[1] / "pyproject.toml").read_text()
        assert "connectors/binding.schema.json" in pyproject
        assert (_PACKAGE / "binding.schema.json").is_file()

    def test_the_shipped_schema_is_the_built_one(self):
        assert load_binding_schema() == binding_schema()


# Values every field is tried with: one of each JSON shape, the edge cases
# where Python and JSON Schema typing differ (384.0, True), and the enum
# members.
_SAMPLES = [
    None,
    "",
    "x",
    "token",
    "ssh_key",
    "token_in_url",
    "ssh_agent",
    "stdio",
    "kubeconfig_prefix",
    0,
    384,
    384.0,
    1.5,
    True,
    [],
    ["a"],
    [1, 2],
    {},
    {"k": "v"},
    {"k": 1},
]


def _agreement_cases():
    """Every fixture, then each one with one part replaced or removed."""
    for form in sorted(VALUE_FIELDS):
        base = _descriptor(form)
        yield f"{form}/valid", base
        for name in VALUE_FIELDS[form]:
            for index, sample in enumerate(_SAMPLES):
                case = copy.deepcopy(base)
                case["entries"][0]["value"][name] = sample
                yield f"{form}/{name}={index}", case
            case = copy.deepcopy(base)
            case["entries"][0]["value"].pop(name, None)
            yield f"{form}/{name}=missing", case
        for key in ("recipient", "collision", "refresh", "retire", "form", "value"):
            for index, sample in enumerate([*_SAMPLES, "workspace", "error", "none"]):
                case = copy.deepcopy(base)
                case["entries"][0][key] = sample
                yield f"{form}/entry.{key}={index}", case
            case = copy.deepcopy(base)
            case["entries"][0].pop(key)
            yield f"{form}/entry.{key}=missing", case
    base = _descriptor("pod_env")
    for key in ("driver", "name", "access", "connector_id", "entries", "extra"):
        for index, sample in enumerate(
            [*_SAMPLES, "srw.x/v1", "srw.x/v1\n", "Srw.x/v1", [{}]]
        ):
            case = copy.deepcopy(base)
            case[key] = sample
            yield f"descriptor.{key}={index}", case
        case = copy.deepcopy(base)
        case.pop(key, None)
        yield f"descriptor.{key}=missing", case


_AGREEMENT_CASES = dict(_agreement_cases())


@pytest.mark.parametrize("case_id", sorted(_AGREEMENT_CASES))
def test_python_and_the_schema_give_the_same_verdict(case_id, schema_validator):
    """Both checks are built from one table; they must accept and refuse the
    same descriptors, including where Python and JSON typing differ."""
    descriptor = _AGREEMENT_CASES[case_id]
    python_valid = validate_binding(descriptor) == []
    schema_valid = not list(schema_validator.iter_errors(descriptor))
    assert python_valid == schema_valid, (python_valid, validate_binding(descriptor))


@pytest.mark.parametrize(
    ("case", "valid"),
    [
        ({"form": "mcp_client", "over": {"args": [1, 2]}}, False),
        ({"form": "credential_file", "over": {"mode": 384.0}}, True),
        ({"form": "credential_file", "over": {"mode": True}}, False),
        ({"form": "mcp_client", "over": {"headers": {"X": 1}}}, False),
    ],
)
def test_the_reported_disagreements_are_settled(case, valid, schema_validator):
    descriptor = _descriptor(case["form"], **case["over"])
    assert (validate_binding(descriptor) == []) is valid
    assert (not list(schema_validator.iter_errors(descriptor))) is valid


def test_a_descriptor_without_access_is_refused_by_both(schema_validator):
    descriptor = _descriptor("pod_env")
    del descriptor["access"]
    assert validate_binding(descriptor)
    assert list(schema_validator.iter_errors(descriptor))


# =============================================================================
# Envelope
# =============================================================================


class TestRequest:
    def test_a_bind_request_round_trips_through_its_validator(self):
        request = DriverRequest(
            operation="bind",
            config={"host": "db"},
            access="ReadOnly",
            credentials={"password": "pw"},
            binding_id="b-1",
            driver_state="opaque",
            execution=ExecutionRef("job", "j-1", "p-1", "sandbox"),
        ).to_json()
        assert request["protocol_version"] == PROTOCOL_VERSION == "1.0"
        assert request["connector"] == {"config": {"host": "db"}, "access": "ReadOnly"}
        assert request["execution"]["workspace_backend"] == "sandbox"
        assert validate_request(request) == []

    def test_secrets_stay_out_of_repr(self):
        request = DriverRequest(
            operation="bind", credentials={"password": "pw"}, driver_state="state"
        )
        assert "pw" not in repr(request)
        assert "state" not in repr(request)

    @pytest.mark.parametrize("operation", ["bind", "revoke", "renew"])
    def test_binding_operations_need_a_binding_id(self, operation):
        request = DriverRequest(operation=operation).to_json()
        assert f"{operation} needs a binding_id" in validate_request(request)

    def test_gc_carries_the_live_binding_ids(self):
        request = DriverRequest(operation="gc", live_binding_ids=("b-1",)).to_json()
        assert request["live_binding_ids"] == ["b-1"]
        assert validate_request(request) == []

    def test_an_unsupported_protocol_major_is_refused(self):
        request = DriverRequest(operation="check", protocol_version="2.0").to_json()
        assert any("protocol_version" in p for p in validate_request(request))

    def test_malformed_requests_are_reported(self):
        problems = validate_request(
            {
                "protocol_version": "1.0",
                "operation": "teleport",
                "connector": {"config": [], "access": None},
                "credentials": None,
                "driver_state": 3,
                "execution": {"kind": "cron"},
            }
        )
        assert len(problems) == 5


def _lines(*objects) -> str:
    return "\n".join(json.dumps(item) for item in objects)


_LOG = {"type": "log", "level": "info", "message": "probing"}
_CHECK_OK = {"type": "result", "result": {"status": "SUCCEEDED", "message": "ok"}}


class TestOutput:
    def test_the_six_error_classes(self):
        assert ERROR_CLASSES == (
            "config",
            "credentials",
            "permission",
            "transient",
            "unsupported",
            "system",
        )

    @pytest.mark.parametrize("error_class", ERROR_CLASSES)
    def test_only_transient_errors_are_retried(self, error_class):
        error = DriverError(error_class, "x")
        assert error.retryable is (error_class == "transient")
        assert DriverError.from_json(error.to_json()) == error

    def test_a_check_result_with_logs(self):
        outcome = read_output(_lines(_LOG, _CHECK_OK), 0, operation="check")
        assert outcome.result == {"status": "SUCCEEDED", "message": "ok"}
        assert outcome.error is None
        assert outcome.logs == (_LOG,)

    def test_a_failed_check_is_a_result_not_an_error(self):
        line = {"type": "result", "result": {"status": "FAILED", "message": "bad"}}
        assert read_output(_lines(line), 0, operation="check").result["status"] == (
            "FAILED"
        )

    def test_a_bind_result_carries_a_valid_binding_and_its_state(self):
        line = {
            "type": "result",
            "result": {"binding": _descriptor("pod_env")},
            "driver_state": "minted-token-id",
        }
        outcome = read_output(_lines(line), 0, operation="bind")
        assert outcome.error is None
        assert outcome.driver_state == "minted-token-id"

    def test_a_bind_result_with_a_broken_binding_is_a_system_error(self):
        line = {"type": "result", "result": {"binding": {"driver": "x"}}}
        outcome = read_output(_lines(line), 0, operation="bind")
        assert outcome.error.error_class == "system"

    def test_an_error_with_updates(self):
        update = {
            "type": "update",
            "target": "credential",
            "slot": "token",
            "value": "n",
        }
        error = {
            "type": "error",
            "error": {
                "class": "transient",
                "message": "rate limited",
                "retry_after_s": 30,
            },
        }
        outcome = read_output(_lines(update, error), 1, operation="renew")
        assert outcome.error == DriverError(
            "transient", "rate limited", retry_after_s=30
        )
        assert outcome.updates == (update,)

    @pytest.mark.parametrize(
        ("stdout", "exit_code"),
        [
            (_lines(_CHECK_OK, _CHECK_OK), 0),
            (_lines(_LOG), 0),
            (_lines(_CHECK_OK), 1),
            ("not json", 0),
            (_lines({"type": "telemetry"}), 0),
            (_lines({"type": "result", "result": {"status": "MAYBE"}}), 0),
            ("x" * (1024 * 1024 + 1), 0),
        ],
    )
    def test_protocol_violations_become_system_errors(self, stdout, exit_code):
        outcome = read_output(stdout, exit_code, operation="check")
        assert outcome.result is None
        assert outcome.error.error_class == "system"

    @pytest.mark.parametrize(
        "line",
        [
            {"type": "log", "level": "loud", "message": "x"},
            {"type": "error", "error": {"class": "cosmic", "message": "x"}},
            {"type": "error", "error": {"class": "config", "message": ""}},
            {"type": "update", "target": "config", "value": []},
            {"type": "update", "target": "credential", "value": "x"},
            {"type": "update", "target": "secret_sauce", "value": "x"},
            {"type": "result", "result": [], "driver_state": 1},
        ],
    )
    def test_malformed_lines_are_refused(self, line):
        with pytest.raises(EnvelopeError):
            parse_output_line(json.dumps(line))


# =============================================================================
# Stdlib only
# =============================================================================


def test_the_contract_imports_only_the_standard_library():
    offenders = []
    for path in sorted(_PACKAGE.glob("*.py")):
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if isinstance(node, ast.Import):
                modules = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom) and node.level == 0:
                modules = [node.module or ""]
            else:
                continue
            for module in modules:
                if module.split(".")[0] not in sys.stdlib_module_names:
                    offenders.append(f"{path.name}: {module}")
    assert offenders == []


# =============================================================================
# Test connection over the API
# =============================================================================


class TestApiCheckResult:
    @pytest.mark.parametrize(
        ("outcome", "expected"),
        [
            (
                DriverOutcome(result={"status": "SUCCEEDED", "message": "Connected"}),
                {"status": "ok", "message": "Connected"},
            ),
            (
                DriverOutcome(result={"status": "FAILED", "message": "No such table"}),
                {"status": "error", "message": "No such table"},
            ),
            (
                DriverOutcome(error=DriverError("unsupported", "No test")),
                {"status": "unsupported", "message": "No test"},
            ),
            (
                DriverOutcome(
                    error=DriverError(
                        "credentials", "Login refused", detail="pw=hunter2"
                    )
                ),
                {
                    "status": "error",
                    "message": "Login refused",
                    "error_class": "credentials",
                },
            ),
        ],
    )
    def test_envelope_outcomes_map_to_api_statuses(self, outcome, expected):
        assert api_check_result(outcome) == expected

    def test_an_image_driver_answer_maps_through_the_same_function(self):
        stdout = json.dumps(
            {"type": "error", "error": {"class": "unsupported", "message": "n/a"}}
        )
        outcome = read_output(stdout, 1, operation="check")
        assert api_check_result(outcome) == {"status": "unsupported", "message": "n/a"}

    def test_built_in_answers_use_only_the_api_statuses(self):
        golden = json.loads(
            (
                Path(__file__).parent / "fixtures" / "connector_goldens" / "probe.json"
            ).read_text()
        )
        statuses = {
            case["body"]["status"]
            for case in golden.values()
            if case["status"] == 200 and "status" in case["body"]
        }
        assert statuses <= set(API_CHECK_STATUSES)
        assert "unsupported" in statuses

    @pytest.mark.parametrize(
        ("status", "headline"),
        [("ok", "OK"), ("unsupported", "NOT TESTABLE"), ("error", "FAILED")],
    )
    def test_the_mcp_tool_says_what_the_status_means(self, status, headline):
        from shared.orch_surface.formatters import format_datasource_test

        text = format_datasource_test("d1", {"status": status, "message": "m"})
        assert text.splitlines()[0] == f"Connector test: {headline}"
