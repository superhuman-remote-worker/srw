"""Test database defaults apply through both imports without requiring Docker."""

import importlib
import shlex
from unittest.mock import MagicMock

import pytest


@pytest.fixture(autouse=True)
def docker_client(monkeypatch):
    monkeypatch.setattr("testcontainers.core.container.DockerClient", MagicMock())


@pytest.mark.parametrize(
    "module_name", ["testcontainers.postgres", "testcontainers.community.postgres"]
)
def test_postgres_disables_disk_sync_by_default(module_name):
    container = importlib.import_module(module_name).PostgresContainer("postgres:15")

    assert shlex.split(container._command) == [
        "postgres",
        "-c",
        "fsync=off",
        "-c",
        "synchronous_commit=off",
        "-c",
        "full_page_writes=off",
    ]


@pytest.mark.parametrize(
    "command", ["postgres -c fsync=on", ["postgres", "-c", "fsync=on"]]
)
def test_postgres_preserves_explicit_constructor_command(command):
    from testcontainers.postgres import PostgresContainer

    container = PostgresContainer("postgres:15", command=command)

    assert container._command == command


def test_postgres_allows_explicit_command_after_construction():
    from testcontainers.postgres import PostgresContainer

    command = "postgres -c fsync=on"
    container = PostgresContainer("postgres:15").with_command(command)

    assert container._command == command
