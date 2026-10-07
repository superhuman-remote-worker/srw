"""The append-only context injection rollback flag (WP2 spec §I).

``context_management.injection_mode`` is ``legacy`` by default and in the
shared root, is validated by both loaders, and ``is_append_only`` reads it
strictly. The carrier fold does not depend on the mode: without entries it
returns the same objects, so legacy requests are unchanged.
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
import yaml
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage

from agent.core.memory_injection import create_memory_injection_messages
from shared.runtime.core.context_entries import (
    INJECTION_MODE_APPEND_ONLY,
    INJECTION_MODE_LEGACY,
    fold_context_entries,
    is_append_only,
)
from shared.runtime.core.loader import (
    ContextManagementConfig,
    load_agent_config,
    load_agent_config_from_dict,
)

_REPO = Path(__file__).resolve().parents[1]


def _minimal(**overrides):
    return {"agent_id": "test_agent", "display_name": "Test Agent", **overrides}


class TestDefault:
    def test_dataclass_default_is_legacy(self):
        assert ContextManagementConfig().injection_mode == INJECTION_MODE_LEGACY

    def test_dict_loader_default_is_legacy(self):
        config = load_agent_config_from_dict(_minimal())
        assert config.context_management.injection_mode == INJECTION_MODE_LEGACY
        assert not is_append_only(config)

    # The bundled default flipped to append_only on dev (D15 measured,
    # 2026-10-07); a config without the key still falls back to legacy above.
    @pytest.mark.parametrize("role_base", ["worker_base", "session_base"])
    def test_bundled_role_bases_are_append_only(self, role_base):
        config = load_agent_config(str(_REPO / "config" / f"{role_base}.yaml"))
        assert config.context_management.injection_mode == INJECTION_MODE_APPEND_ONLY
        assert is_append_only(config)

    def test_the_shared_root_states_the_key(self):
        root = yaml.safe_load((_REPO / "config" / "expert_base.yaml").read_text())
        assert root["context_management"]["injection_mode"] == "append_only"


class TestParsing:
    def test_dict_loader_reads_append_only(self):
        config = load_agent_config_from_dict(
            _minimal(context_management={"injection_mode": "append_only"})
        )
        assert config.context_management.injection_mode == INJECTION_MODE_APPEND_ONLY
        assert is_append_only(config)

    def test_file_loader_reads_append_only(self, tmp_path):
        leaf = tmp_path / "leaf.yaml"
        leaf.write_text(
            yaml.safe_dump(
                {
                    "$extends": "worker_base",
                    "agent_id": "flag_test",
                    "display_name": "Flag test",
                    "context_management": {"injection_mode": "append_only"},
                }
            )
        )
        config = load_agent_config(str(leaf))
        assert is_append_only(config)
        # The rest of the section still comes from the chain.
        assert config.context_management.max_summary_length == 20000

    @pytest.mark.parametrize("value", ["append-only", "APPEND_ONLY", "on", True, 1])
    def test_dict_loader_rejects_other_values(self, value):
        with pytest.raises(ValueError, match="injection_mode"):
            load_agent_config_from_dict(
                _minimal(context_management={"injection_mode": value})
            )

    def test_file_loader_rejects_other_values(self, tmp_path):
        leaf = tmp_path / "leaf.yaml"
        leaf.write_text(
            yaml.safe_dump(
                {
                    "$extends": "worker_base",
                    "agent_id": "flag_test",
                    "display_name": "Flag test",
                    "context_management": {"injection_mode": "sometimes"},
                }
            )
        )
        with pytest.raises(ValueError, match="injection_mode"):
            load_agent_config(str(leaf))


class TestIsAppendOnly:
    def test_a_mock_config_is_legacy(self):
        assert not is_append_only(MagicMock())

    @pytest.mark.parametrize(
        "config",
        [
            None,
            SimpleNamespace(),
            SimpleNamespace(context_management=None),
            SimpleNamespace(context_management=SimpleNamespace()),
            SimpleNamespace(
                context_management=SimpleNamespace(injection_mode="legacy")
            ),
        ],
    )
    def test_anything_but_the_exact_value_is_legacy(self, config):
        assert not is_append_only(config)

    def test_the_exact_value_is_append_only(self):
        config = SimpleNamespace(
            context_management=SimpleNamespace(injection_mode="append_only")
        )
        assert is_append_only(config)


def test_fold_of_an_entry_free_request_returns_the_same_objects():
    mem_ai, mem_tool = create_memory_injection_messages("--- Memories ---")
    request = [
        SystemMessage(content="system"),
        HumanMessage(content="task"),
        AIMessage(
            content="", tool_calls=[{"id": "c1", "name": "read_file", "args": {}}]
        ),
        ToolMessage(content="file", tool_call_id="c1"),
        mem_ai,
        mem_tool,
    ]
    folded = fold_context_entries(request)
    assert len(folded) == len(request)
    assert all(a is b for a, b in zip(folded, request))
