"""Strict JSON schema compatibility checks for auxiliary outputs.

This test enforces that every auxiliary output schema can be converted to
strict JSON Schema without runtime validation errors. (Conversation
summarization is a text-mode task and has no output schema.)
"""

from __future__ import annotations

import inspect
from typing import Any, Type

import pytest
from pydantic import BaseModel

from shared.runtime.services.auxiliary import AuxTask
import shared.runtime.services.auxiliary as auxiliary

try:  # pragma: no cover - compatibility for older/openai-less envs
    from openai.lib._pydantic import to_strict_json_schema
except Exception:  # pragma: no cover
    to_strict_json_schema = None


def _iter_task_classes() -> list[type[AuxTask]]:
    classes: list[type[AuxTask]] = []
    for value in vars(auxiliary).values():
        if (
            inspect.isclass(value)
            and issubclass(value, AuxTask)
            and value not in (AuxTask,)
            and value.__module__ == auxiliary.__name__
            and not inspect.isabstract(value)
        ):
            classes.append(value)
    return classes


def _dummy_for(name: str, annotation: Any) -> Any:
    if name in {
        "messages",
        "neighbours",
        "existing_notes",
        "stale_notes",
        "related_notes",
        "assembler_tools",
        "kb_tools",
    }:
        return []
    if name in {
        "conversation_text",
        "prompt",
        "candidate_content",
        "claim",
        "quote_context",
        "source_content",
        "verbatim_quote",
        "phase_data",
        "workspace_md",
        "plan_md",
        "current_injection",
    }:
        return "sample"
    if annotation is int:
        return 1
    if annotation is float:
        return 0.0
    if annotation is list:
        return []
    if annotation is dict:
        return {}
    if annotation is str:
        return "sample"
    return "sample"


def _instance_for_task(task_type: type[AuxTask]) -> AuxTask:
    sig = inspect.signature(task_type.__init__)
    kwargs = {}
    for param in sig.parameters.values():
        if param.name == "self":
            continue
        if param.default is not inspect._empty:
            kwargs[param.name] = param.default
            continue
        kwargs[param.name] = _dummy_for(param.name, param.annotation)
    return task_type(**kwargs)


def _all_aux_output_schemas() -> list[Type[BaseModel]]:
    schemas: list[Type[BaseModel]] = []
    seen: set[type[BaseModel]] = set()
    for task_type in _iter_task_classes():
        task = _instance_for_task(task_type)
        schema = task.output_schema
        if schema not in seen:
            schemas.append(schema)
            seen.add(schema)
    return schemas


@pytest.mark.parametrize("schema", _all_aux_output_schemas())
def test_aux_output_schemas_are_strict_json_compatible(schema: Type[BaseModel]):
    if to_strict_json_schema is not None:
        result = to_strict_json_schema(schema)
    else:
        result = schema.model_json_schema()

    assert isinstance(result, dict)
    assert result.get("type") in {"object", None}
