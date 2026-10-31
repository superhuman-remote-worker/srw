"""Old-layout synthetic bytes must retain state, message types and identifiers.

No live data is embedded. The negative custom-model records deliberately prove
that a successful decode can lose a type, making the DB inventory a real gate.
"""

import base64
import importlib
import importlib.util
from importlib.metadata import version
import json
from pathlib import Path
import sys

from langchain_core.messages import (
    AIMessage,
    HumanMessage,
    SystemMessage,
    ToolMessage,
    message_to_dict,
    messages_from_dict,
)
from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer
import ormsgpack
import pytest


ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "tests/fixtures/source_flattening/checkpoint_4_1_1.json"
_spec = importlib.util.spec_from_file_location(
    "_checkpoint_type_inventory", ROOT / "scripts/checkpoint_type_inventory.py"
)
assert _spec and _spec.loader
inventory = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = inventory
_spec.loader.exec_module(inventory)


@pytest.fixture
def fixture():
    document = json.loads(FIXTURE.read_text())
    assert document["synthetic_only"] is True
    assert document["producer_versions"]["langgraph-checkpoint"] == "4.1.1"
    assert version("langgraph-checkpoint") == "4.3.0"
    return document


def _bytes(record):
    return record["encoding"], base64.b64decode(record["blob_base64"], validate=True)


def _guard_old_module_imports(monkeypatch):
    original = importlib.import_module

    def guarded(name, package=None):
        assert name != "src" and not name.startswith("src."), (
            "compatibility proof must not import an old first-party module"
        )
        return original(name, package)

    monkeypatch.setattr(importlib, "import_module", guarded)


def test_old_worker_state_restores_actual_values_and_message_types(
    fixture, monkeypatch
):
    _guard_old_module_imports(monkeypatch)
    record = next(
        record for record in fixture["records"] if record["name"] == "worker-state"
    )
    decoded = JsonPlusSerializer(
        allowed_msgpack_modules=None, pickle_fallback=False
    ).loads_typed(_bytes(record))
    assert {
        key: value for key, value in decoded.items() if key != "messages"
    } == record["expected"]["fields"]
    assert [type(message) for message in decoded["messages"]] == [
        SystemMessage,
        HumanMessage,
        AIMessage,
        ToolMessage,
    ]
    assert [message_to_dict(message) for message in decoded["messages"]] == record[
        "expected"
    ]["messages"]
    assert (
        decoded["messages"][2].tool_calls[0]["id"]
        == decoded["messages"][3].tool_call_id
    )
    assert decoded["worker_resume_id"] == "synthetic-resume"
    assert decoded["delivered_reply_keys"] == ["synthetic-reply"]


@pytest.mark.parametrize("name", ("pending-write-message", "pending-write-completion"))
def test_pending_write_values_restore_without_first_party_imports(
    fixture, monkeypatch, name
):
    _guard_old_module_imports(monkeypatch)
    record = next(record for record in fixture["records"] if record["name"] == name)
    decoded = JsonPlusSerializer(
        allowed_msgpack_modules=None, pickle_fallback=False
    ).loads_typed(_bytes(record))
    if name == "pending-write-message":
        assert type(decoded) is AIMessage
        assert message_to_dict(decoded) == record["expected"]
    else:
        assert type(decoded) is dict
        assert decoded == record["expected"]


def test_inventory_flags_old_models_even_when_strict_decode_succeeds(fixture):
    scanner = inventory.Inventory()
    serializer = JsonPlusSerializer(allowed_msgpack_modules=None, pickle_fallback=False)
    for record in fixture["records"]:
        if record["expectation"] == "first-party-type-requires-decision":
            scanner.add(*_bytes(record))
            decoded = serializer.loads_typed(_bytes(record))
            assert type(decoded) is dict  # the original Pydantic type was lost
            assert decoded == record["expected"]
    summary = scanner.summary()
    assert summary["errors"] == {}
    assert {
        (ref["module"], ref["class"]) for ref in summary["first_party_references"]
    } == {
        ("src.core.context", "IdentityAnchor"),
        ("src.core.context", "ConversationSummary"),
    }


def test_positive_fixture_inventory_has_no_first_party_types(fixture):
    scanner = inventory.Inventory()
    for record in fixture["records"]:
        if record["expectation"] == "preserve":
            scanner.add(*_bytes(record))
    summary = scanner.summary()
    assert summary["rows"] == 3
    assert summary["errors"] == {}
    assert summary["first_party_references"] == []
    assert {ref["class"] for ref in summary["type_references"]} == {
        "SystemMessage",
        "HumanMessage",
        "AIMessage",
        "ToolMessage",
    }


def test_inventory_never_imports_stored_constructor_and_reports_corruption(monkeypatch):
    def forbidden(*_args, **_kwargs):
        raise AssertionError("the inventory must not call stored constructors")

    monkeypatch.setattr(importlib, "import_module", forbidden)
    scanner = inventory.Inventory()
    payload = ormsgpack.packb(
        ormsgpack.Ext(
            0, ormsgpack.packb(("src.fake", "Constructor", "sensitive fixture value"))
        )
    )
    scanner.add("msgpack", payload)
    scanner.add("pickle", b"not decoded")
    scanner.add("msgpack", b"\xc1")
    summary = scanner.summary()
    assert summary["first_party_references"] == [
        {"module": "src.fake", "class": "Constructor", "count": 1}
    ]
    assert summary["errors"]["unsupported_encoding"] == 1
    assert len(summary["errors"]) == 2
    assert "sensitive fixture value" not in json.dumps(summary)


def test_inventory_handles_legacy_json_and_primitive_encodings():
    scanner = inventory.Inventory()
    scanner.add(
        "json",
        json.dumps(
            {
                "lc": 2,
                "type": "constructor",
                "id": ["src", "core", "context", "IdentityAnchor"],
                "kwargs": {"agent_role": "private fixture data"},
            }
        ).encode(),
    )
    for encoding in ("null", "empty", "bytes", "bytearray"):
        scanner.add(encoding, b"")
    summary = scanner.summary()
    assert summary["errors"] == {}
    assert summary["first_party_references"] == [
        {"module": "src.core.context", "class": "IdentityAnchor", "count": 1}
    ]
    assert "private fixture data" not in json.dumps(summary)


def test_subagent_fork_seed_retains_provider_content_and_identity(fixture):
    envelope = fixture["subagent_fork_seed"]["provider_raw"][
        "_srw_subagent_fork_seed_v1"
    ]
    restored = messages_from_dict([envelope])[0]
    assert type(restored) is HumanMessage
    assert restored.id == "synthetic-user"
    assert restored.content == [{"type": "text", "text": "Read the synthetic note."}]
    assert restored.additional_kwargs == {"fixture_identity": "synthetic"}
    assert message_to_dict(restored) == envelope
