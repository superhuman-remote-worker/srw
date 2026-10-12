"""Containers are sized only by the selected WorkspaceTemplate."""

import logging

from fastapi import HTTPException
import pytest

from orchestrator.services.config_overrides import (
    refuse_execution_owned_workspace_keys,
)
from shared.runtime.core.workspace_selection import bind_execution_workspace


@pytest.mark.parametrize("key", ["container", "sandbox"])
def test_caller_overrides_cannot_size_a_container(key):
    with pytest.raises(HTTPException) as denied:
        refuse_execution_owned_workspace_keys(
            {"workspace": {key: {"image": "registry.example/x:1"}}}
        )
    assert denied.value.status_code == 422
    assert (
        "are no longer supported. Put the image and resources in a WorkspaceTemplate"
        in denied.value.detail
    )


@pytest.mark.parametrize(
    "override", [None, {}, {"workspace": {"backend": "sandbox"}}, {"tools": {}}]
)
def test_ordinary_overrides_pass(override):
    refuse_execution_owned_workspace_keys(override)


def test_bind_drops_legacy_container_settings(caplog):
    with caplog.at_level(logging.WARNING):
        bound = bind_execution_workspace(
            {"workspace": {"container": {"cpu": "2"}, "max_read_words": 5}},
            {"backend": "sandbox"},
        )
    assert bound["workspace"] == {"max_read_words": 5, "backend": "sandbox"}
    assert "workspace.container" in caplog.text
