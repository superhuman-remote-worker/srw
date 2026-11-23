"""Preserve structured-recovery caller policy and lightweight imports."""

import subprocess
import sys
from types import SimpleNamespace
from typing import Literal
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
from pydantic import BaseModel, RootModel

from orchestrator.services import message_triage
from shared.runtime.llm.structured_recovery import recover_structured
from shared.runtime.services.auxiliary import AuxiliaryLLM


class Decision(BaseModel):
    action: Literal["interrupt", "queue"]
    reason: str


VALID = {"action": "interrupt", "reason": "urgent"}


@pytest.mark.parametrize(
    "module",
    ["shared.runtime.llm.structured_recovery", "orchestrator.services.message_triage"],
)
def test_recovery_imports_do_not_initialize_applications_or_model_frameworks(
    module, tmp_path
):
    code = """
import importlib
import importlib.abc
import sys

forbidden = ("agent", "orchestrator.main", "langchain", "langchain_core",
             "langchain_openai", "langgraph", "shared.runtime.services.auxiliary",
             "shared.runtime.core.loader")
class RejectHeavyImport(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if any(fullname == prefix or fullname.startswith(prefix + ".")
               for prefix in forbidden):
            raise AssertionError("Unexpected recovery dependency: " + fullname)
sys.meta_path.insert(0, RejectHeavyImport())
importlib.import_module(sys.argv[1])
assert not any(name == prefix or name.startswith(prefix + ".")
               for name in sys.modules for prefix in forbidden)
"""
    result = subprocess.run(
        [sys.executable, "-I", "-c", code, module],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize(
    ("raw", "triage_expected", "typed_expected"),
    [
        (None, None, None),
        ("", None, None),
        ("not json", None, None),
        ('{"action":"interrupt","reason":"urgent"}', VALID, VALID),
        ('prefix {"action":"interrupt","reason":"urgent"} trailing', VALID, VALID),
        ('```json\n{"action":"interrupt","reason":"urgent"}\n```', VALID, VALID),
        ('```JSON\n{"action":"interrupt","reason":"urgent"}\n```', VALID, VALID),
        ('```\n{"action":"interrupt","reason":"urgent"}\n```', VALID, VALID),
        (
            '<THINK>{"action":"queue"}\nignore</THINK>\n{"action":"interrupt","reason":"urgent"}',
            VALID,
            VALID,
        ),
        ('<think>{"action":"interrupt","reason":"urgent"}</think>', None, None),
        ('{"action":"interrupt","reason":"<think>hidden</think>urgent"}', VALID, VALID),
        ('{"action":"interrupt","reason":"urgent"} {"action":"queue"}', VALID, VALID),
        ('{bad} {"action":"interrupt","reason":"urgent"}', None, None),
        ('{] {"action":"interrupt","reason":"urgent"}', None, None),
        ('{"action":"interrupt","reason":"unfinished', None, None),
        ('"unfinished quote {"action":"interrupt","reason":"urgent"}', None, None),
        (r'\{"action":"interrupt","reason":"urgent"}', None, None),
        (
            '{"action":"INTERRUPT","reason":"urgent"}',
            {"action": "INTERRUPT", "reason": "urgent"},
            None,
        ),
        (
            '{"action":"other","reason":"urgent"}',
            {"action": "queue", "reason": "urgent"},
            None,
        ),
        (
            '{"action":null,"reason":"urgent"}',
            {"action": "queue", "reason": "urgent"},
            None,
        ),
        ('{"reason":"urgent"}', {"reason": "urgent"}, None),
        ('{"action":"interrupt"}', {"action": "interrupt"}, None),
        ("{}", {}, None),
        (
            '{"action":"interrupt","reason":null}',
            {"action": "interrupt", "reason": None},
            None,
        ),
        ('[{"action":"interrupt","reason":"urgent"}]', None, None),
        (
            r'{"action":"interrupt","reason":"quoted \"} [\" and \\ slash"}',
            {"action": "interrupt", "reason": 'quoted "} [" and \\ slash'},
            {"action": "interrupt", "reason": 'quoted "} [" and \\ slash'},
        ),
        (
            '{"action":"interrupt","reason":"urgent","nested":[{"closing":"}]"}]}',
            {**VALID, "nested": [{"closing": "}]"}]},
            VALID,
        ),
    ],
)
def test_existing_callers_keep_extraction_and_validation_policies(
    raw, triage_expected, typed_expected
):
    assert message_triage._recover_structured_json(raw) == triage_expected
    typed = recover_structured(raw, Decision)
    assert (typed.model_dump() if typed is not None else None) == typed_expected


def test_schema_owner_can_accept_an_array_rejected_by_triage():
    raw = '[{"action":"interrupt","reason":"urgent"}]'
    schema = RootModel[list[Decision]]
    assert recover_structured(raw, schema).model_dump() == [VALID]
    assert message_triage._recover_structured_json(raw) is None


@pytest.mark.parametrize("raw", [123, b'{"action":"interrupt"}', ["text"]])
def test_truthy_non_text_still_raises_before_validation(raw):
    with pytest.raises(TypeError):
        message_triage._recover_structured_json(raw)
    with pytest.raises(TypeError):
        recover_structured(raw, Decision)


def test_schema_ordinary_exceptions_stay_best_effort():
    class FailingSchema:
        @classmethod
        def model_validate_json(cls, _candidate):
            raise RuntimeError("validation failed")

    assert recover_structured('{"action":"interrupt"}', FailingSchema) is None


def test_auxiliary_recovery_preserves_result_and_error_contracts():
    wrapper = AuxiliaryLLM(llm=MagicMock())
    raw = SimpleNamespace(
        content='```json\n{"action":"interrupt","reason":"urgent"}\n```'
    )
    recovered = wrapper._recover_structured_output(
        {"raw": raw, "parsed": None, "parsing_error": "original"},
        Decision,
        "triage-probe",
    )
    assert recovered == {"raw": raw, "parsed": Decision(**VALID), "parsing_error": None}
    assert recovered["raw"] is raw
    with pytest.raises(
        ValueError,
        match="Structured-output validation failed for triage-probe: original",
    ):
        wrapper._recover_structured_output(
            {
                "raw": SimpleNamespace(content='{"action":"other"}'),
                "parsed": None,
                "parsing_error": "original",
            },
            Decision,
            "triage-probe",
        )
    with pytest.raises(
        TypeError, match="Unexpected structured-output result for triage-probe"
    ):
        wrapper._recover_structured_output("raw", Decision, "triage-probe")


@pytest.mark.parametrize(
    ("content", "expected"),
    [
        ('{"action":"interrupt","reason":"urgent"}', VALID),
        (
            '{"action":"INTERRUPT","reason":"urgent"}',
            {"action": "queue", "reason": "urgent"},
        ),
        (
            '{"action":"other","reason":"urgent"}',
            {"action": "queue", "reason": "urgent"},
        ),
        ('{"reason":"urgent"}', {"action": "queue", "reason": "urgent"}),
        ('{"action":"interrupt"}', {"action": "interrupt", "reason": ""}),
        (
            '{"action":"interrupt","reason":null}',
            {"action": "interrupt", "reason": None},
        ),
        ("[{}]", {"action": "queue", "reason": "triage unavailable"}),
        ("not json", {"action": "queue", "reason": "triage unavailable"}),
    ],
)
@pytest.mark.asyncio
async def test_public_triage_keeps_action_defaults_and_advisory_fallback(
    monkeypatch, content, expected
):
    response = httpx.Response(
        200,
        json={"choices": [{"message": {"content": content}}]},
        request=httpx.Request("POST", "https://fixture.invalid/chat/completions"),
    )
    transport = SimpleNamespace(post=AsyncMock(return_value=response))
    context = MagicMock()
    context.__aenter__ = AsyncMock(return_value=transport)
    context.__aexit__ = AsyncMock(return_value=False)
    monkeypatch.setattr(
        message_triage.httpx, "AsyncClient", MagicMock(return_value=context)
    )
    monkeypatch.setattr(
        message_triage,
        "_resolve_triage_config",
        AsyncMock(
            return_value=(
                "fixture-model",
                "https://fixture.invalid",
                "fixture-key",
                None,
            )
        ),
    )
    assert (
        await message_triage.triage_message("hello", "processing", "job", db=object())
        == expected
    )
    transport.post.assert_awaited_once()
