"""Reading the chart's declaration of built-in workspace templates."""

import json
from unittest.mock import AsyncMock

import pytest

from orchestrator.services import builtin_workspace_templates as builtins

DOCUMENT = {
    "apiVersion": "srw/v1alpha1",
    "kind": "WorkspaceTemplate",
    "metadata": {"name": "virtual", "scope": {"kind": "Catalog", "name": "shared"}},
    "spec": {"backend": "virtual"},
}


def test_the_variable_name_is_the_charts():
    assert builtins.ENV_NAME == "WORKSPACE_BUILTIN_TEMPLATES"


@pytest.mark.parametrize(
    "environ", [{}, {builtins.ENV_NAME: ""}, {builtins.ENV_NAME: "  "}]
)
def test_an_absent_declaration_means_do_nothing(environ):
    assert builtins.declared_builtin_templates(environ) is None


def test_an_empty_list_is_a_declaration():
    assert builtins.declared_builtin_templates({builtins.ENV_NAME: "[]"}) == []


def test_a_list_of_documents_is_returned():
    environ = {builtins.ENV_NAME: json.dumps([DOCUMENT])}
    assert builtins.declared_builtin_templates(environ) == [DOCUMENT]


@pytest.mark.parametrize(
    "raw", ["not json", "{}", '"text"', "[1]", '[{"kind": "x"}, "y"]', "null"]
)
def test_anything_else_is_an_error(raw):
    with pytest.raises(ValueError):
        builtins.declared_builtin_templates({builtins.ENV_NAME: raw})


@pytest.mark.asyncio
@pytest.mark.parametrize("raw", ["not json", "{}", "[1]"])
async def test_malformed_declaration_never_stops_startup(monkeypatch, caplog, raw):
    monkeypatch.setenv(builtins.ENV_NAME, raw)
    database = AsyncMock()
    assert (
        await builtins.reconcile_builtin_workspace_templates_at_startup(database)
        is None
    )
    assert database.mock_calls == []
    assert "Built-in workspace templates were not reconciled" in caplog.text


@pytest.mark.asyncio
async def test_an_absent_declaration_never_touches_the_database(monkeypatch):
    monkeypatch.delenv(builtins.ENV_NAME, raising=False)
    database = AsyncMock()
    assert (
        await builtins.reconcile_builtin_workspace_templates_at_startup(database)
        is None
    )
    assert database.mock_calls == []


@pytest.mark.asyncio
async def test_a_database_failure_never_stops_startup(monkeypatch, caplog):
    monkeypatch.setenv(builtins.ENV_NAME, json.dumps([DOCUMENT]))
    monkeypatch.setattr(
        builtins,
        "reconcile_builtin_workspace_templates",
        AsyncMock(side_effect=RuntimeError("database is down")),
    )
    assert (
        await builtins.reconcile_builtin_workspace_templates_at_startup(AsyncMock())
        is None
    )
    assert "Built-in workspace templates were not reconciled" in caplog.text
