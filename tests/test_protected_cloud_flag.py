from __future__ import annotations


import pytest
from orchestrator.services import deployment_gates as deployment_gates_module


@pytest.mark.parametrize(
    "value,expected",
    [
        ("true", True),
        ("TRUE", True),
        (" 1 ", True),
        ("yes", True),
        ("false", False),
        ("0", False),
        ("", False),
        ("off", False),
    ],
)
def test_protected_cloud_mode_flag_parsing(monkeypatch, value, expected):
    monkeypatch.setenv("PROTECTED_CLOUD_MODE_ENABLED", value)
    assert deployment_gates_module.is_protected_cloud_mode_enabled() is expected


def test_protected_cloud_mode_flag_absent_defaults_false(monkeypatch):
    monkeypatch.delenv("PROTECTED_CLOUD_MODE_ENABLED", raising=False)
    assert deployment_gates_module.is_protected_cloud_mode_enabled() is False
